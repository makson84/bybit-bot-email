import os
import json
import time
import asyncio
import threading
import pickle
import numpy as np
from datetime import datetime
from collections import deque
from queue import Queue
import requests
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ==================== НАСТРОЙКИ ПОЧТЫ ====================
EMAIL_FROM = "maksut-1984@yandex.ru"
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD', 'dzvxcfnfahmagjdt')
EMAIL_TO = "maksut-1984@yandex.ru"
SMTP_SERVER = "smtp.yandex.ru"
SMTP_PORT = 465

# ==================== НАСТРОЙКИ БОТА ====================
TIMEFRAME = 15
BB_PERIOD = 20
BB_STD_DEV = 2
BB_BUFFER_PCT = 1.0
VOLUME_24H_THRESHOLD = 1000000
RSI_PERIOD = 14
RSI_OVERBOUGHT = 0
RSI_OVERSOLD = 0
CHECK_INTERVAL = 30  # Проверка каждые 30 секунд

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
symbol_buffers = {}
symbol_volume_buffers = {}
signal_cache = {}
cache_lock = threading.Lock()
live_volumes = {}
volumes_lock = threading.Lock()
bollinger_cache = {}
urgent_queue = Queue()
messages_sent_today = 0
reset_day = datetime.now().day
send_lock = threading.Lock()

stats = {
    "total_signals": 0,
    "sent_signals": 0,
    "start_time": datetime.now()
}

# ==================== ОТПРАВКА НА ПОЧТУ ====================
def send_email_signal(signal):
    global messages_sent_today
    with send_lock:
        current_day = datetime.now().day
        global reset_day
        if current_day != reset_day:
            messages_sent_today = 0
            reset_day = current_day
        
        if signal.get('volume_24h', 0) < VOLUME_24H_THRESHOLD:
            return False
        
        emoji = "🟢 LONG" if signal['side'] == 'LONG' else "🔴 SHORT"
        subject = f"{emoji} {signal['side']} {signal['symbol']} | {signal['deviation']:.2f}%"
        
        body = f"""
{emoji} МГНОВЕННЫЙ СИГНАЛ: {signal['side']} | {signal['symbol']}
⏰ Время: {signal['candle_time']}
📈 Цена касания: {signal['price']:.6f}
🎯 Отклонение: {signal['deviation']:.2f}%
📊 RSI: {signal['rsi']:.1f}
💎 Объём 24ч: {signal['volume_24h']:,.0f} USDT
📊 Верхняя BB: {signal['bb_high']:.6f}
📊 Нижняя BB: {signal['bb_low']:.6f}
━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ СИГНАЛ В МОМЕНТЕ КАСАНИЯ!
        """
        
        try:
            msg = MIMEMultipart()
            msg['From'] = EMAIL_FROM
            msg['To'] = EMAIL_TO
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
                server.login(EMAIL_FROM, EMAIL_PASSWORD)
                server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
            
            messages_sent_today += 1
            stats['sent_signals'] += 1
            print(f"✅ Отправлено на почту: {signal['side']} {signal['symbol']}")
            return True
        except Exception as e:
            print(f"❌ Ошибка отправки письма: {e}")
            return False

def send_email_message(text):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = EMAIL_TO
        msg['Subject'] = "📊 Статус бота"
        msg.attach(MIMEText(text, 'plain', 'utf-8'))
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        return True
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def load_symbols_cache():
    try:
        if os.path.exists("symbols_cache_email.pkl"):
            with open("symbols_cache_email.pkl", "rb") as f:
                cache = pickle.load(f)
                if cache.get('symbols'):
                    return cache['symbols']
    except:
        pass
    return None

def get_all_usdt_futures():
    cached = load_symbols_cache()
    if cached:
        print(f"📋 Загружено {len(cached)} пар из кэша")
        return cached
    
    pairs = []
    try:
        resp = requests.get("https://api.bybit.com/v5/market/instruments-info", params={"category": "linear", "limit": 1000}, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('retCode') == 0:
                for item in data['result']['list']:
                    symbol = item.get('symbol')
                    status = item.get('status')
                    quote = item.get('quoteCoin')
                    if symbol and status == 'Trading' and quote == 'USDT':
                        pairs.append(symbol)
                print(f"✅ Получено {len(pairs)} пар")
                return pairs
    except Exception as e:
        print(f"⚠️ Ошибка получения списка: {e}")
    
    return ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT']

def fetch_klines(symbol, limit=100):
    for attempt in range(2):
        try:
            resp = requests.get("https://api.bybit.com/v5/market/kline", params={"category": "linear", "symbol": symbol, "interval": "15", "limit": limit}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('retCode') == 0:
                    klines = data['result']['list']
                    klines.reverse()
                    return klines
        except:
            time.sleep(0.5)
    return None

def init_symbol_data(symbols):
    global symbol_buffers, symbol_volume_buffers
    print(f"⏳ Начинаю загрузку {len(symbols)} пар...")
    for idx, sym in enumerate(symbols, 1):
        if idx % 50 == 0:
            print(f"📊 Прогресс: {idx}/{len(symbols)}")
        klines = fetch_klines(sym, limit=100)
        if klines:
            closes = [float(k[4]) for k in klines]
            symbol_buffers[sym] = deque(closes, maxlen=200)
            turnovers = [float(k[6]) for k in klines[-96:]]
            symbol_volume_buffers[sym] = deque(turnovers, maxlen=96)
        time.sleep(0.02)
    print(f"✅ Загружено {len(symbol_buffers)} пар")

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes)
    gain = np.where(deltas > 0, deltas, 0)
    loss = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gain[-period:])
    avg_loss = np.mean(loss[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_bollinger(symbol):
    closes = list(symbol_buffers.get(symbol, []))
    if len(closes) < BB_PERIOD:
        return None, None
    prev = closes[-BB_PERIOD:]
    sma = np.mean(prev)
    std = np.std(prev, ddof=0)
    return sma + BB_STD_DEV * std, sma - BB_STD_DEV * std

def check_signal_instant(symbol, candle, bb_high, bb_low, volume_24h):
    try:
        high_price = float(candle['high'])
        low_price = float(candle['low'])
        
        short_cond = high_price >= (bb_high * (1 + BB_BUFFER_PCT / 100))
        long_cond = low_price <= (bb_low * (1 - BB_BUFFER_PCT / 100))
        
        if not short_cond and not long_cond:
            return None
        
        closes = list(symbol_buffers.get(symbol, []))
        rsi = calculate_rsi(closes) if len(closes) > RSI_PERIOD else 50
        
        if RSI_OVERBOUGHT > 0 and short_cond and rsi < RSI_OVERBOUGHT:
            return None
        if RSI_OVERSOLD > 0 and long_cond and rsi > RSI_OVERSOLD:
            return None
        
        signal = None
        if short_cond:
            deviation = ((high_price / bb_high) - 1) * 100
            signal = {
                'side': 'SHORT',
                'symbol': symbol,
                'price': high_price,
                'bb_high': bb_high,
                'bb_low': bb_low,
                'deviation': deviation,
                'volume_24h': volume_24h,
                'rsi': rsi,
                'candle_time': datetime.now().strftime('%H:%M:%S')
            }
        elif long_cond:
            deviation = ((bb_low / low_price) - 1) * 100
            signal = {
                'side': 'LONG',
                'symbol': symbol,
                'price': low_price,
                'bb_high': bb_high,
                'bb_low': bb_low,
                'deviation': deviation,
                'volume_24h': volume_24h,
                'rsi': rsi,
                'candle_time': datetime.now().strftime('%H:%M:%S')
            }
        
        if signal:
            stats['total_signals'] += 1
            print(f"⚡ МГНОВЕННЫЙ СИГНАЛ {signal['side']} {symbol} (откл: {signal['deviation']:.2f}%, RSI: {rsi:.1f})")
            send_email_signal(signal)
            return signal
        return None
    except Exception as e:
        print(f"⚠️ Ошибка: {e}")
        return None

async def fetch_volumes():
    while True:
        try:
            resp = requests.get("https://api.bybit.com/v5/market/tickers", params={"category": "linear"}, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('retCode') == 0:
                    with volumes_lock:
                        live_volumes.clear()
                        for item in data['result']['list']:
                            live_volumes[item['symbol']] = float(item.get('turnover24h', 0))
                    print(f"📊 Обновлены объёмы для {len(live_volumes)} пар")
        except:
            pass
        await asyncio.sleep(60)

async def check_symbols():
    while True:
        try:
            symbols = list(symbol_buffers.keys())
            if not symbols:
                await asyncio.sleep(5)
                continue
            
            print(f"🔄 Проверка {len(symbols)} пар (мгновенные сигналы)...")
            
            for symbol in symbols:
                try:
                    # Получаем текущую цену (для мгновенных сигналов)
                    ticker_resp = requests.get(
                        "https://api.bybit.com/v5/market/tickers",
                        params={"category": "linear", "symbol": symbol},
                        timeout=5
                    )
                    current_price = None
                    if ticker_resp.status_code == 200:
                        ticker_data = ticker_resp.json()
                        if ticker_data.get('retCode') == 0:
                            tickers = ticker_data['result']['list']
                            if tickers:
                                current_price = float(tickers[0].get('lastPrice', 0))
                    
                    # Получаем последнюю свечу
                    resp = requests.get(
                        "https://api.bybit.com/v5/market/kline",
                        params={"category": "linear", "symbol": symbol, "interval": "1", "limit": 2},
                        timeout=10
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get('retCode') == 0:
                            klines = data['result']['list']
                            if klines:
                                candle = klines[-1]
                                candle = {
                                    'high': float(candle[2]),
                                    'low': float(candle[3]),
                                    'close': float(candle[4]),
                                    'start': str(int(candle[0])),
                                    'turnover': float(candle[6])
                                }
                                
                                close_price = candle['close']
                                turnover = candle['turnover']
                                if symbol in symbol_buffers:
                                    symbol_buffers[symbol].append(close_price)
                                if symbol in symbol_volume_buffers:
                                    symbol_volume_buffers[symbol].append(turnover)
                                
                                bb = bollinger_cache.get(symbol)
                                if bb:
                                    bb_high, bb_low = bb
                                    with volumes_lock:
                                        volume_24h = live_volumes.get(symbol, 0)
                                    
                                    # Проверка по свече
                                    check_signal_instant(symbol, candle, bb_high, bb_low, volume_24h)
                                    
                                    # Мгновенная проверка по текущей цене (если есть)
                                    if current_price:
                                        fake_candle = {
                                            'high': str(current_price),
                                            'low': str(current_price),
                                            'close': str(current_price),
                                            'start': str(int(time.time() * 1000)),
                                            'turnover': str(turnover)
                                        }
                                        check_signal_instant(symbol, fake_candle, bb_high, bb_low, volume_24h)
                                    
                except Exception as e:
                    pass
                
                await asyncio.sleep(0.02)  # Задержка между монетами
            
            await asyncio.sleep(CHECK_INTERVAL)
            
        except Exception as e:
            print(f"⚠️ Ошибка в check_symbols: {e}")
            await asyncio.sleep(10)

async def monitor_active_pairs():
    global bollinger_cache
    while True:
        try:
            active = list(symbol_buffers.keys())
            if not active:
                await asyncio.sleep(5)
                continue
            
            bollinger_cache = {}
            for sym in active:
                if sym in symbol_buffers:
                    bb_high, bb_low = calculate_bollinger(sym)
                    if bb_high:
                        bollinger_cache[sym] = (bb_high, bb_low)
            
            print(f"📐 Рассчитаны полосы для {len(bollinger_cache)} пар")
            await asyncio.sleep(60)
        except Exception as e:
            print(f"⚠️ Ошибка: {e}")
            await asyncio.sleep(10)

def stats_printer():
    while True:
        time.sleep(60)
        runtime = datetime.now() - stats['start_time']
        print(f"\n📊 СТАТИСТИКА:")
        print(f"   Сигналов: {stats['total_signals']}")
        print(f"   Отправлено: {stats['sent_signals']}")
        print(f"   Всего пар: {len(symbol_buffers)}")
        print(f"   Сегодня: {messages_sent_today}")
        print(f"   Время: {runtime}\n")

async def main():
    print("=" * 60)
    print("🤖 БОТ-СКАНЕР BOLLINGER + RSI (МГНОВЕННЫЕ СИГНАЛЫ)")
    print("=" * 60)
    print(f"📊 НАСТРОЙКИ:")
    print(f"   Таймфрейм: {TIMEFRAME} мин")
    print(f"   Буфер: {BB_BUFFER_PCT}%")
    print(f"   RSI: {RSI_OVERBOUGHT}/{RSI_OVERSOLD}")
    print(f"   Объём: > {VOLUME_24H_THRESHOLD:,.0f} USDT")
    print("=" * 60)
    print(f"📧 Письма идут на: {EMAIL_TO}")
    print("=" * 60)
    
    send_email_message("🚀 БОТ ЗАПУЩЕН! (МГНОВЕННЫЕ СИГНАЛЫ)")
    
    threading.Thread(target=stats_printer, daemon=True).start()
    
    symbols = get_all_usdt_futures()
    if not symbols:
        send_email_message("❌ Ошибка: нет пар")
        return
    
    init_symbol_data(symbols)
    send_email_message(f"✅ Загружено {len(symbol_buffers)} пар")
    
    volume_task = asyncio.create_task(fetch_volumes())
    monitor_task = asyncio.create_task(monitor_active_pairs())
    check_task = asyncio.create_task(check_symbols())
    
    send_email_message("✅ Бот работает! Мгновенные сигналы при касании")
    
    try:
        await asyncio.gather(volume_task, monitor_task, check_task)
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлен")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
