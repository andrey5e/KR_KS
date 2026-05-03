import re
import uuid
from datetime import datetime
from typing import Optional, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from jose import jwt
from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship

DATABASE_URL = "sqlite:///./car_service.db"
Base = declarative_base()
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class User(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True)
    username = Column(String, unique=True, index=True)
    full_name = Column(String, default="")
    password = Column(String)
    role = Column(String)
    phone = Column(String, default="")
    specialization = Column(String, default="")
    orders_as_client = relationship("Order", foreign_keys="Order.client_id", cascade="all, delete-orphan")
    stages_assigned = relationship("Stage", foreign_keys="Stage.assigned_master_id")

class Order(Base):
    __tablename__ = "orders"
    id = Column(String, primary_key=True)
    client_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    car_model = Column(String)
    car_plate = Column(String)
    status = Column(String, default="created")
    current_stage_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    stages = relationship("Stage", cascade="all, delete-orphan")
    messages = relationship("Message", cascade="all, delete-orphan")

class Stage(Base):
    __tablename__ = "stages"
    id = Column(String, primary_key=True)
    order_id = Column(String, ForeignKey("orders.id", ondelete="CASCADE"))
    name = Column(String)
    description = Column(Text, default="")
    assigned_master_id = Column(String, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    status = Column(String, default="pending")
    order_index = Column(Integer)

class Message(Base):
    __tablename__ = "messages"
    id = Column(String, primary_key=True)
    order_id = Column(String, ForeignKey("orders.id", ondelete="CASCADE"))
    chat_type = Column(String)
    sender_id = Column(String)
    sender_name = Column(String)
    sender_role = Column(String)
    text = Column(Text)
    timestamp = Column(DateTime, default=datetime.now)

Base.metadata.create_all(bind=engine)

class LoginRequest(BaseModel):
    username: str
    password: str

class RegisterRequest(BaseModel):
    username: str
    full_name: str
    password: str
    role: str
    phone: str = ""
    specialization: str = ""

    @field_validator('phone')
    @classmethod
    def validate_phone(cls, v: str) -> str:
        if v and not re.match(r'^\+7 \(\d{3}\) \d{3}-\d{2}-\d{2}$', v):
            raise ValueError('Номер должен быть в формате +7 (XXX) XXX-XX-XX')
        return v

class UpdateUserRequest(BaseModel):
    username: Optional[str] = None
    full_name: Optional[str] = None
    password: Optional[str] = None
    phone: Optional[str] = None
    specialization: Optional[str] = None

    @field_validator('phone')
    @classmethod
    def validate_phone(cls, v: Optional[str]) -> Optional[str]:
        if v and not re.match(r'^\+7 \(\d{3}\) \d{3}-\d{2}-\d{2}$', v):
            raise ValueError('Номер должен быть в формате +7 (XXX) XXX-XX-XX')
        return v

class StageItem(BaseModel):
    name: str
    description: str = ""

class CreateOrderRequest(BaseModel):
    client_id: str
    car_model: str
    car_plate: str
    stages: List[StageItem]
    token: str

    @field_validator('car_plate')
    @classmethod
    def validate_car_plate(cls, v: str) -> str:
        v = v.upper()
        pattern = r'^[АВЕКМНОРСТУХABEKMHOPCTYX] \d{3} [АВЕКМНОРСТУХABEKMHOPCTYX]{2} \d{2,3}$'
        if not re.match(pattern, v):
            raise ValueError('Гос. номер должен быть в формате "А 777 АА 77"')
        return v

class UpdateOrderStagesRequest(BaseModel):
    stages: List[StageItem]
    token: str

class StartStageRequest(BaseModel):
    token: str

class CompleteStageRequest(BaseModel):
    token: str

SECRET_KEY = "super-secret-key"
ALGORITHM = "HS256"
active_connections = {}

SERVICES_TO_MASTER = {
    "Мойка автомобиля": "master1",
    "Оклейка кузова плёнкой": "master1",
    "Оклейка отдельных элементов": "master1",
    "Аэрография": "master1",
    "Установка обвеса": "master2",
    "Покраска дисков": "master2",
    "Полировка фар": "master2",
    "Тонировка стёкол": "master2",
    "Чип-тюнинг двигателя": "master3",
    "Тюнинг КПП": "master3",
    "Тюнинг выхлопной системы": "master3",
    "Тюнинг подвески": "master3",
    "Тюнинг тормозной системы": "master3",
    "Установка доп. освещения": "master4",
    "Тюнинг фар головного света": "master4",
    "Подсветка салона": "master4",
    "Неоновая подсветка": "master4",
    "Профессиональная химчистка": "master5",
    "Перетяжка салона": "master5",
    "Шумоизоляция салона": "master5",
    "Установка системы мультимедиа": "master5"
}

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def new_id() -> str:
    return str(uuid.uuid4())[:8]

def create_token(user_id: str) -> str:
    return jwt.encode({"user_id": user_id}, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("user_id")
    except:
        return None

async def send_to_user(user_id: str, message: dict):
    if user_id in active_connections:
        for ws in active_connections[user_id]:
            try:
                await ws.send_json(message)
            except:
                pass

async def notify_client_order_status(order_id: str, message_text: str, db: Session):
    order = db.query(Order).filter(Order.id == order_id).first()
    if order:
        await send_to_user(order.client_id, {
            "type": "status_update",
            "data": {"order_id": order_id, "text": message_text, "timestamp": datetime.now().isoformat()}
        })

async def try_auto_start_next_stage(order_id: str, db: Session):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        return
    stages_for_order = db.query(Stage).filter(Stage.order_id == order_id).order_by(Stage.order_index).all()
    current = next((s for s in stages_for_order if s.id == order.current_stage_id), None)
    next_stage = None
    for s in stages_for_order:
        if current and s.order_index > current.order_index and s.status != "completed":
            next_stage = s
            break
        elif not current and s.status != "completed":
            next_stage = s
            break
    if next_stage:
        next_stage.status = "pending"
        order.current_stage_id = next_stage.id
        db.commit()
        await notify_client_order_status(order_id, f"Ожидается начало этапа: {next_stage.name}", db)
    else:
        order.status = "completed"
        db.commit()
        await notify_client_order_status(order_id, "Автомобиль готов! Спасибо за обращение в наше Тюнинг-ателье!", db)

@asynccontextmanager
async def lifespan(app: FastAPI):
    db = SessionLocal()
    if not db.query(User).first():
        db.add_all([
            User(id="admin", username="admin", full_name="Андрей Евгеньевич", password="admin", role="admin"),
            User(id="master1", username="Мастер_1", full_name="Иван", password="master1", role="master", specialization="Внешний тюнинг"),
            User(id="master2", username="Мастер_2", full_name="Сергей", password="master2", role="master", specialization="Внешний тюнинг"),
            User(id="master3", username="Мастер_3", full_name="Алексей", password="master3", role="master", specialization="Технический тюнинг"),
            User(id="master4", username="Мастер_4", full_name="Дмитрий", password="master4", role="master", specialization="Тюнинг освещения"),
            User(id="master5", username="Мастер_5", full_name="Евгений", password="master5", role="master", specialization="Тюнинг интерьера"),
        ])
        db.commit()
    db.close()
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/auth/login")
async def login(req: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == req.username, User.password == req.password).first()
    if user:
        return {"token": create_token(user.id), "user_id": user.id, "role": user.role, "full_name": user.full_name, "specialization": user.specialization}
    raise HTTPException(401, "Неверный логин или пароль")

@app.post("/auth/register")
async def register(req: RegisterRequest, db: Session = Depends(get_db)):
    if db.query(User).filter(User.username == req.username).first():
        raise HTTPException(400, "Пользователь с таким именем уже существует")
    user_id = new_id()
    new_user = User(
        id=user_id,
        username=req.username,
        full_name=req.full_name,
        password=req.password,
        role=req.role,
        phone=req.phone,
        specialization=req.specialization
    )
    db.add(new_user)
    db.commit()
    return {"token": create_token(user_id), "user_id": user_id, "role": req.role, "full_name": req.full_name}

@app.get("/api/users")
async def list_users(role: str, token: str, db: Session = Depends(get_db)):
    requester_id = decode_token(token)
    requester = db.query(User).filter(User.id == requester_id).first()
    if not requester or requester.role != "admin":
        raise HTTPException(403, "Доступ запрещён")
    users = db.query(User).filter(User.role == role).all()
    return [{"id": u.id, "username": u.username, "full_name": u.full_name, "phone": u.phone, "specialization": u.specialization} for u in users]

@app.put("/api/users/{user_id}")
async def update_user(user_id: str, req: UpdateUserRequest, token: str, db: Session = Depends(get_db)):
    requester_id = decode_token(token)
    requester = db.query(User).filter(User.id == requester_id).first()
    if not requester or requester.role != "admin":
        raise HTTPException(403, "Только администратор может редактировать пользователей")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "Пользователь не найден")
    if req.username is not None:
        user.username = req.username
    if req.full_name is not None:
        user.full_name = req.full_name
    if req.password is not None:
        user.password = req.password
    if req.phone is not None:
        user.phone = req.phone
    if req.specialization is not None:
        user.specialization = req.specialization
    db.commit()
    return {"message": "Данные обновлены"}

@app.delete("/api/users/{user_id}")
async def delete_user(user_id: str, token: str, db: Session = Depends(get_db)):
    requester_id = decode_token(token)
    requester = db.query(User).filter(User.id == requester_id).first()
    if not requester or requester.role != "admin":
        raise HTTPException(403, "Только администратор может удалять пользователей")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "Пользователь не найден")
    if user.id == requester_id:
        raise HTTPException(400, "Нельзя удалить свой собственный аккаунт")
    db.delete(user)
    db.commit()
    return {"message": "Пользователь удалён"}

@app.get("/api/services")
async def get_services():
    return [{"name": name, "assigned_master_id": master_id} for name, master_id in SERVICES_TO_MASTER.items()]

@app.post("/api/orders")
async def create_order(req: CreateOrderRequest, db: Session = Depends(get_db)):
    uid = decode_token(req.token)
    admin = db.query(User).filter(User.id == uid, User.role == "admin").first()
    if not admin:
        raise HTTPException(403, "Только администратор может создавать заказы")
    client = db.query(User).filter(User.username == req.client_id).first()
    if not client:
        raise HTTPException(400, "Клиент не найден")
    if not req.stages:
        raise HTTPException(400, "Должен быть хотя бы один этап")
    order_id = new_id()
    new_order = Order(id=order_id, client_id=client.id, car_model=req.car_model, car_plate=req.car_plate.upper(), status="created")
    db.add(new_order)
    for idx, stg in enumerate(req.stages):
        stage_id = new_id()
        assigned_master_id = SERVICES_TO_MASTER.get(stg.name)
        if not assigned_master_id:
            raise HTTPException(400, f"Услуга '{stg.name}' не найдена в списке допустимых. Создание заказа отменено.")
        stage = Stage(
            id=stage_id,
            order_id=order_id,
            name=stg.name,
            description=stg.description,
            assigned_master_id=assigned_master_id,
            status="pending",
            order_index=idx
        )
        db.add(stage)
        if idx == 0:
            new_order.current_stage_id = stage_id
    db.commit()
    await notify_client_order_status(order_id, f"Автомобиль {req.car_model} принят в работу", db)
    return {"order_id": order_id}

@app.get("/api/orders")
async def list_orders(token: str, db: Session = Depends(get_db)):
    uid = decode_token(token)
    user = db.query(User).filter(User.id == uid).first()
    if not user:
        raise HTTPException(401, "Недействительный токен")
    if user.role == "admin":
        orders = db.query(Order).all()
    elif user.role == "client":
        orders = db.query(Order).filter(Order.client_id == uid).all()
    elif user.role == "master":
        stages = db.query(Stage).filter(Stage.assigned_master_id == uid).all()
        order_ids = {s.order_id for s in stages}
        orders = db.query(Order).filter(Order.id.in_(order_ids)).all()
    else:
        orders = []
    result = []
    for o in orders:
        client = db.query(User).filter(User.id == o.client_id).first()
        result.append({
            "id": o.id,
            "client_id": client.username if client else o.client_id,
            "car_model": o.car_model,
            "car_plate": o.car_plate,
            "status": o.status,
            "current_stage_id": o.current_stage_id,
            "created_at": o.created_at.isoformat()
        })
    return result

@app.delete("/api/orders/{order_id}")
async def delete_order(order_id: str, token: str, db: Session = Depends(get_db)):
    uid = decode_token(token)
    admin = db.query(User).filter(User.id == uid, User.role == "admin").first()
    if not admin:
        raise HTTPException(403, "Только администратор может удалять заказы")
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(404, "Заказ не найден")
    db.delete(order)
    db.commit()
    return {"message": "Заказ успешно удален"}

@app.put("/api/orders/{order_id}/stages")
async def update_order_stages(order_id: str, req: UpdateOrderStagesRequest, db: Session = Depends(get_db)):
    uid = decode_token(req.token)
    admin = db.query(User).filter(User.id == uid, User.role == "admin").first()
    if not admin:
        raise HTTPException(403, "Только администратор может изменять услуги заказа")
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(404, "Заказ не найден")
    if not req.stages:
        raise HTTPException(400, "В заказе должен быть хотя бы один этап")
    new_stages_data = []
    for idx, stg in enumerate(req.stages):
        master_id = SERVICES_TO_MASTER.get(stg.name)
        if not master_id:
            raise HTTPException(400, f"Услуга '{stg.name}' не найдена в списке допустимых.")
        new_stages_data.append((stg, master_id, idx))
    db.query(Stage).filter(Stage.order_id == order_id).delete()
    first_stage_id = None
    for stg, master_id, idx in new_stages_data:
        stage_id = new_id()
        if idx == 0:
            first_stage_id = stage_id
        db.add(Stage(
            id=stage_id,
            order_id=order_id,
            name=stg.name,
            description=stg.description,
            assigned_master_id=master_id,
            status="pending",
            order_index=idx
        ))
    order.current_stage_id = first_stage_id
    order.status = "created"
    db.commit()
    await notify_client_order_status(order_id, "Администратор изменил набор услуг. Статус заказа обновлён.", db)
    return {"message": "Услуги заказа успешно обновлены"}

@app.get("/api/orders/{order_id}/stages")
async def get_order_stages(order_id: str, token: str, db: Session = Depends(get_db)):
    uid = decode_token(token)
    if not uid:
        raise HTTPException(401, "Недействительный токен")
    stages = db.query(Stage).filter(Stage.order_id == order_id).order_by(Stage.order_index).all()
    result = []
    for s in stages:
        master = db.query(User).filter(User.id == s.assigned_master_id).first() if s.assigned_master_id else None
        result.append({
            "id": s.id,
            "name": s.name,
            "status": s.status,
            "assigned_master_name": master.full_name if master else "Не назначен",
            "assigned_master_id": s.assigned_master_id
        })
    return result

@app.get("/api/my_stages")
async def get_my_stages(token: str, db: Session = Depends(get_db)):
    uid = decode_token(token)
    if not uid:
        raise HTTPException(401, "Недействительный токен")
    user = db.query(User).filter(User.id == uid).first()
    if user.role != "master":
        raise HTTPException(403, "Только для мастеров")
    my_stages = db.query(Stage).filter(Stage.assigned_master_id == uid).all()
    result = []
    for s in my_stages:
        order = db.query(Order).filter(Order.id == s.order_id).first()
        if order:
            result.append({
                "stage": {"id": s.id, "name": s.name, "status": s.status, "order_index": s.order_index},
                "order": {"id": order.id, "car_model": order.car_model, "car_plate": order.car_plate}
            })
    return result

@app.post("/api/stages/{stage_id}/start")
async def start_stage(stage_id: str, req: StartStageRequest, db: Session = Depends(get_db)):
    uid = decode_token(req.token)
    stage = db.query(Stage).filter(Stage.id == stage_id).first()
    if not stage or stage.assigned_master_id != uid:
        raise HTTPException(403, "Это не ваш этап")
    if stage.status != "pending":
        raise HTTPException(400, "Этап уже начат или завершён")
    stage.status = "in_progress"
    order = db.query(Order).filter(Order.id == stage.order_id).first()
    order.status = "in_progress"
    order.current_stage_id = stage_id
    db.commit()
    await notify_client_order_status(stage.order_id, f"Начат этап: {stage.name}", db)
    return {"ok": True}

@app.post("/api/stages/{stage_id}/complete")
async def complete_stage(stage_id: str, req: CompleteStageRequest, db: Session = Depends(get_db)):
    uid = decode_token(req.token)
    stage = db.query(Stage).filter(Stage.id == stage_id).first()
    if not stage or stage.assigned_master_id != uid:
        raise HTTPException(403, "Это не ваш этап")
    if stage.status != "in_progress":
        raise HTTPException(400, "Этап не в процессе выполнения")
    stage.status = "completed"
    db.commit()
    await notify_client_order_status(stage.order_id, f"Завершён этап: {stage.name}", db)
    await try_auto_start_next_stage(stage.order_id, db)
    return {"ok": True}

@app.get("/api/orders/{order_id}/messages")
async def get_order_messages(order_id: str, token: str, db: Session = Depends(get_db)):
    uid = decode_token(token)
    if not uid:
        raise HTTPException(401, "Недействительный токен")
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(404, "Заказ не найден")
    user = db.query(User).filter(User.id == uid).first()
    if user.role not in ["admin", "client"]:
        if user.role == "master":
            stage = db.query(Stage).filter(Stage.order_id == order_id, Stage.assigned_master_id == uid).first()
            if not stage:
                raise HTTPException(403, "Нет доступа к сообщениям этого заказа")
        else:
            raise HTTPException(403, "Доступ запрещён")
    if user.role == "client" and order.client_id != uid:
        raise HTTPException(403, "Это не ваш заказ")
    msgs = db.query(Message).filter(Message.order_id == order_id).order_by(Message.timestamp).all()
    return [{"id": m.id, "sender_id": m.sender_id, "sender_name": m.sender_name, "sender_role": m.sender_role, "text": m.text, "timestamp": m.timestamp.isoformat()} for m in msgs]

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str):
    user_id = decode_token(token)
    if not user_id:
        return await websocket.close(code=1008)
    with SessionLocal() as db:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return await websocket.close(code=1008)
    await websocket.accept()
    if user_id not in active_connections:
        active_connections[user_id] = []
    active_connections[user_id].append(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "chat":
                order_id = data.get("order_id")
                text = data.get("text")
                if not order_id or not text:
                    continue
                with SessionLocal() as db:
                    order = db.query(Order).filter(Order.id == order_id).first()
                    if not order:
                        continue
                    recipients = {order.client_id}
                    admins = db.query(User).filter(User.role == "admin").all()
                    for a in admins:
                        recipients.add(a.id)
                    current_stage = db.query(Stage).filter(Stage.id == order.current_stage_id).first()
                    if current_stage and current_stage.assigned_master_id:
                        recipients.add(current_stage.assigned_master_id)
                    recipients.add(user_id)
                    msg_obj = Message(
                        id=new_id(),
                        order_id=order_id,
                        chat_type="general",
                        sender_id=user_id,
                        sender_name=user.full_name or user.username,
                        sender_role=user.role,
                        text=text
                    )
                    db.add(msg_obj)
                    db.commit()
                    msg_dict = {
                        "order_id": order_id,
                        "sender_id": msg_obj.sender_id,
                        "sender_name": msg_obj.sender_name,
                        "sender_role": msg_obj.sender_role,
                        "text": msg_obj.text,
                        "timestamp": msg_obj.timestamp.isoformat()
                    }
                for rid in recipients:
                    await send_to_user(rid, {"type": "chat", "data": msg_dict})
    except WebSocketDisconnect:
        if user_id in active_connections:
            active_connections[user_id].remove(websocket)
            if not active_connections[user_id]:
                del active_connections[user_id]

app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")

@app.get("/")
async def root():
    return FileResponse("frontend/login.html")
