
import os
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram import F
import asyncio

TOKEN = os.getenv("7872394424:AAGwGb-oSmM31NAEg-NG5uZEiPauFhQTSXo")
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class FactoryForm(StatesGroup):
    inn = State()
    category = State()

@dp.message(Command("start"))
async def start(message: Message, state: FSMContext):
    kb = types.ReplyKeyboardMarkup(keyboard=[
        [types.KeyboardButton(text="Я — фабрика")],
        [types.KeyboardButton(text="Мне нужна фабрика")]
    ], resize_keyboard=True)
    await message.answer("Привет! Кто вы?", reply_markup=kb)

@dp.message(F.text == "Я — фабрика")
async def register_factory(message: Message, state: FSMContext):
    await message.answer("Введите ваш ИНН:")
    await state.set_state(FactoryForm.inn)

@dp.message(FactoryForm.inn)
async def inn_received(message: Message, state: FSMContext):
    await state.update_data(inn=message.text)
    await message.answer("Какие категории вы шьёте?")
    await state.set_state(FactoryForm.category)

@dp.message(FactoryForm.category)
async def category_received(message: Message, state: FSMContext):
    data = await state.get_data()
    await message.answer(f"✅ Фабрика зарегистрирована!\nИНН: {data['inn']}\nКатегории: {message.text}")
    await state.clear()

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
