import os
import json
import time
import asyncio
import threading
import pickle
import zlib
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
EMAIL_PASSWORD = "dzvxcfnfahmagjdt"
EMAIL_TO = "maksut-1984@yandex.ru"
SMTP_SERVER = "smtp.yandex.ru"
SMTP_PORT = 465

# ==================== НАСТРОЙКИ БОТА ====================
TIMEFRAME = 15
BB_PERIOD = 20
BB_STD_DEV = 2
BB_BUFFER_PCT = 3.0
VOLUME_24H_THRESHOLD = 1000000
RSI_PERIOD = 14
RSI_OVERBOUGHT = 75
RSI_OVERSOLD = 35
MONITOR_DURATION = 60
UPDATE_INTERVAL = 3600
WHITE_LIST = []
BLACK_LIST = ["USDCUSDT", "TUSDUSDT", "BUSDUSDT", "DAIUSDT"]
AUTO_RESTART = True
RESTART_DELAY = 30
CHECK_INTERVAL = 60

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
all_symbols = []
is_loading = False
failed_symbols_list = []

stats = {
    "total_signals": 0,
    "sent_signals": 0,
    "filtered_signals": 0,
    "start_time": datetime.now(),
    "errors": 0,
    "restarts": 0
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
{emoji} СИГНАЛ: {signal['side']} | {signal['symbol']}
⏰ Время: {signal['candle_time']}
📈 Цена: {signal['price']:.6f}
🎯 Отклонение: {signal['deviation']:.2f}%
📊 RSI: {signal['rsi']:.1f}
💎 Объём 24ч: {signal['volume_24h']:,.0f} USDT
📊 Верхняя BB: {signal['bb_high']:.6f}
📊 Нижняя BB: {signal['bb_low']:.6f}
━━━━━━━━━━━━━━━━━━━━━━━━━
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

# ==================== ФУНКЦИИ БОТА ====================
def load_cache():
    global signal_cache
    if os.path.exists("signal_cache_email.pkl"):
        try:
            with open("signal_cache_email.pkl", "rb") as f:
                signal_cache = pickle.load(f)
            now = time.time()
            to_delete = [k for k, v in signal_cache.items() if now - v > 7200]
            for k in to_delete:
                del signal_cache[k]
        except:
            signal_cache = {}

def save_cache():
    with cache_lock:
        try:
            with open("signal_cache_email.pkl", "wb") as f:
                pickle.dump(signal_cache, f)
        except:
            pass

def can_send(symbol, start_time):
    key = f"{symbol}_{start_time}"
    with cache_lock:
        if key in signal_cache:
            return False
        signal_cache[key] = time.time()
        return True

def save_symbols_cache(symbols):
    try:
        cache = {'symbols': symbols, 'timestamp': time.time()}
        with open("symbols_cache_email.pkl", "wb") as f:
            pickle.dump(cache, f)
    except:
        pass

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
        return cached
    pairs = []
    for attempt in range(3):
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
                            if WHITE_LIST and symbol not in WHITE_LIST:
                                continue
                            if BLACK_LIST and symbol in BLACK_LIST:
                                continue
                            pairs.append(symbol)
                    if pairs:
                        save_symbols_cache(pairs)
                        return pairs
        except:
            time.sleep(2)
    return ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT']

def fetch_klines(symbol):
    for attempt in range(2):
        try:
            resp = requests.get("https://api.bybit.com/v5/market/kline", params={"category": "linear", "symbol": symbol, "interval": str(TIMEFRAME), "limit": 100}, timeout=15)
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
    global symbol_buffers, symbol_volume_buffers, all_symbols, is_loading, failed_symbols_list
    is_loading = True
    all_symbols = symbols
    total = len(symbols)
    try:
        if os.path.exists("history_cache_email.pkl"):
            with open("history_cache_email.pkl", "rb") as f:
                cache = pickle.load(f)
                if cache.get('symbols') == symbols:
                    symbol_buffers = cache['buffers']
                    symbol_volume_buffers = cache['volume_buffers']
                    print(f"⚡ Загружено {len(symbol_buffers)} пар из кэша (МГНОВЕННО!)")
                    is_loading = False
                    return
    except:
        pass
    print(f"⏳ Начинаю загрузку {total} пар...")
    failed_symbols_list = []
    loaded_count = 0
    for idx, sym in enumerate(symbols, 1):
        if idx % 50 == 0:
            print(f"📊 Прогресс: {idx}/{total} ({idx*100//total}%) загружено: {loaded_count}")
        try:
            klines = fetch_klines(sym)
            if klines:
                closes = [float(k[4]) for k in klines]
                symbol_buffers[sym] = deque(closes, maxlen=100)
                turnovers = [float(k[6]) for k in klines[-96:]]
                symbol_volume_buffers[sym] = deque(turnovers, maxlen=96)
                loaded_count += 1
            else:
                failed_symbols_list.append(sym)
        except:
            failed_symbols_list.append(sym)
        time.sleep(0.02)
    try:
        cache = {'symbols': list(symbol_buffers.keys()), 'buffers': symbol_buffers, 'volume_buffers': symbol_volume_buffers}
        with open("history_cache_email.pkl", "wb") as f:
            pickle.dump(cache, f)
    except:
        pass
    if failed_symbols_list:
        print(f"⚠️ Пропущено {len(failed_symbols_list)} пар: {failed_symbols_list[:10]}...")
    is_loading = False
    print(f"✅ Загружено {len(symbol_buffers)} пар")

def calculate_rsi(closes, period=RSI_PERIOD):
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
            signal = {'side': 'SHORT', 'symbol': symbol, 'price': high_price, 'bb_high': bb_high, 'bb_low': bb_low, 'deviation': deviation, 'volume_24h': volume_24h, 'rsi': rsi, 'start_time': candle['start'], 'candle_time': datetime.now().strftime('%H:%M:%S')}
        elif long_cond:
            deviation = ((bb_low / low_price) - 1) * 100
            signal = {'side': 'LONG', 'symbol': symbol, 'price': low_price, 'bb_high': bb_high, 'bb_low': bb_low, 'deviation': deviation, 'volume_24h': volume_24h, 'rsi': rsi, 'start_time': candle['start'], 'candle_time': datetime.now().strftime('%H:%M:%S')}
        if signal and can_send(symbol, candle['start']):
            stats['total_signals'] += 1
            print(f"⚡ СИГНАЛ {signal['side']} {symbol} (откл: {signal['deviation']:.2f}%, RSI: {rsi:.1f})")
            send_email_signal(signal)
            return signal
        return None
    except:
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
            
            total = len(symbols)
            print(f"🔄 Начинаю проверку {total} пар...")
            
            batch_size = 20
            batches = [symbols[i:i+batch_size] for i in range(0, total, batch_size)]
            
            for batch_idx, batch in enumerate(batches):
                for symbol in batch:
                    try:
                        resp = requests.get(
                            "https://api.bybit.com/v5/market/kline",
                            params={
                                "category": "linear", 
                                "symbol": symbol, 
                                "interval": str(TIMEFRAME), 
                                "limit": 2
                            },
                            timeout=10
                        )
                        if resp.status_code == 200:
                            data = resp.json()
                            if data.get('retCode') == 0:
                                klines = data['result']['list']
                                if klines:
                                    candle = klines[-1]
                                    candle = {
                                        'high': candle[2],
                                        'low': candle[3],
                                        'close': candle[4],
                                        'start': str(int(candle[0])),
                                        'turnover': candle[6]
                                    }
                                    
                                    close_price = float(candle['close'])
                                    turnover = float(candle['turnover'])
                                    if symbol in symbol_buffers:
                                        symbol_buffers[symbol].append(close_price)
                                    if symbol in symbol_volume_buffers:
                                        symbol_volume_buffers[symbol].append(turnover)
                                    
                                    bb = bollinger_cache.get(symbol)
                                    if bb:
                                        bb_high, bb_low = bb
                                        with volumes_lock:
                                            volume_24h = live_volumes.get(symbol, 0)
                                        
                                        check_signal_instant(symbol, candle, bb_high, bb_low, volume_24h)
                    except:
                        pass
                    
                    await asyncio.sleep(0.1)
                
                await asyncio.sleep(2)
            
            print(f"✅ Проверка {total} пар завершена")
            
        except Exception as e:
            print(f"⚠️ Ошибка в check_symbols: {e}")
        
        await asyncio.sleep(CHECK_INTERVAL)

async def monitor_active_pairs():
    global bollinger_cache
    while True:
        try:
            active = list(symbol_buffers.keys())
            if not active:
                await asyncio.sleep(5)
                continue
            
            bollinger_cache = {}
            count = 0
            for sym in active:
                if sym in symbol_buffers:
                    bb_high, bb_low = calculate_bollinger(sym)
                    if bb_high:
                        bollinger_cache[sym] = (bb_high, bb_low)
                        count += 1
            
            print(f"📐 Рассчитаны полосы для {count} пар")
            await asyncio.sleep(60)
            
        except Exception as e:
            print(f"⚠️ Ошибка в monitor_active_pairs: {e}")
            await asyncio.sleep(10)

async def update_symbols_periodically():
    while True:
        await asyncio.sleep(UPDATE_INTERVAL)
        try:
            new_symbols = get_all_usdt_futures()
            if new_symbols:
                current = set(symbol_buffers.keys())
                new_pairs = set(new_symbols) - current
                if new_pairs:
                    print(f"🆕 Найдено {len(new_pairs)} новых пар")
                    for sym in list(new_pairs)[:20]:
                        klines = fetch_klines(sym)
                        if klines:
                            closes = [float(k[4]) for k in klines]
                            symbol_buffers[sym] = deque(closes, maxlen=100)
                            turnovers = [float(k[6]) for k in klines[-96:]]
                            symbol_volume_buffers[sym] = deque(turnovers, maxlen=96)
                        await asyncio.sleep(0.1)
                    all_symbols = list(current | new_pairs)
                    save_symbols_cache(all_symbols)
        except:
            pass

def stats_printer():
    while True:
        time.sleep(60)
        runtime = datetime.now() - stats['start_time']
        print(f"\n📊 СТАТИСТИКА:")
        print(f"   Сигналов: {stats['total_signals']}")
        print(f"   Отправлено: {stats['sent_signals']}")
        print(f"   Всего пар: {len(symbol_buffers)}")
        print(f"   Перезапусков: {stats['restarts']}")
        print(f"   Сегодня: {messages_sent_today}")
        print(f"   Время: {runtime}\n")

async def main():
    print("=" * 60)
    print("🤖 БОТ-СКАНЕР BOLLINGER + RSI (EMAIL УВЕДОМЛЕНИЯ)")
    print("=" * 60)
    print(f"📊 НАСТРОЙКИ:")
    print(f"   Таймфрейм: {TIMEFRAME} мин")
    print(f"   Буфер: {BB_BUFFER_PCT}%")
    print(f"   RSI: {RSI_OVERBOUGHT}/{RSI_OVERSOLD}")
    print(f"   Объём: > {VOLUME_24H_THRESHOLD:,.0f} USDT")
    print("=" * 60)
    print(f"📧 Письма идут на: {EMAIL_TO}")
    print("=" * 60)
    
    send_email_message("🚀 БОТ ЗАПУЩЕН! (Email уведомления)")
    
    threading.Thread(target=stats_printer, daemon=True).start()
    
    load_cache()
    
    try:
        symbols = get_all_usdt_futures()
        if not symbols:
            send_email_message("❌ Ошибка: нет пар")
            return
        
        init_symbol_data(symbols)
        send_email_message(f"✅ Загружено {len(symbol_buffers)} пар")
        
        volume_task = asyncio.create_task(fetch_volumes())
        monitor_task = asyncio.create_task(monitor_active_pairs())
        check_task = asyncio.create_task(check_symbols())
        update_task = asyncio.create_task(update_symbols_periodically())
        
        send_email_message("✅ Бот работает! Проверка всех пар")
        
        try:
            await asyncio.gather(volume_task, monitor_task, check_task, update_task)
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            save_cache()
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        save_cache()
    finally:
        save_cache()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлен")
        send_email_message("🛑 Бот остановлен пользователем")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        print("✅ Выход")