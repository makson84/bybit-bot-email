import os
import json
import time
import asyncio
import threading
import pickle
import numpy as np
from datetime import datetime
from collections import deque
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys

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
BB_BUFFER_PCT = -10.0          # ТЕСТОВЫЙ РЕЖИМ
VOLUME_24H_THRESHOLD = 0
RSI_PERIOD = 14
RSI_OVERBOUGHT = 0
RSI_OVERSOLD = 0

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
symbol_buffers = {}
symbol_volume_buffers = {}
live_volumes = {}
bollinger_cache = {}
messages_sent_today = 0
reset_day = datetime.now().day

stats = {
    "total_signals": 0,
    "sent_signals": 0,
    "start_time": datetime.now()
}

# ==================== ОТПРАВКА НА ПОЧТУ ====================
def send_email_signal(signal):
    global messages_sent_today, reset_day
    current_day = datetime.now().day
    if current_day != reset_day:
        messages_sent_today = 0
        reset_day = current_day
    
    if VOLUME_24H_THRESHOLD > 0:
        vol = signal.get('volume_24h', 0)
        if vol < VOLUME_24H_THRESHOLD:
            print(f"⏩ {signal['symbol']}: объём {vol:,.0f} < {VOLUME_24H_THRESHOLD:,.0f} (пропуск)")
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
        print(f"❌ Ошибка отправки: {e}")
        return False

# ==================== КЭШ ПАР ====================
def load_symbols_cache():
    try:
        if os.path.exists("symbols_cache_email.pkl"):
            with open("symbols_cache_email.pkl", "rb") as f:
                data = pickle.load(f)
                if data and 'symbols' in data:
                    print(f"📦 Кэш списка загружен: {len(data['symbols'])} пар")
                    return data['symbols']
    except Exception as e:
        print(f"⚠️ Ошибка загрузки кэша списка: {e}")
    return None

def save_symbols_cache(symbols):
    try:
        with open("symbols_cache_email.pkl", "wb") as f:
            pickle.dump({'symbols': symbols, 'timestamp': time.time()}, f)
        print(f"💾 Кэш списка сохранён: {len(symbols)} пар")
    except Exception as e:
        print(f"⚠️ Ошибка сохранения кэша списка: {e}")

# ==================== ПОЛУЧЕНИЕ СПИСКА ПАР ====================
def get_all_usdt_futures():
    cached = load_symbols_cache()
    if cached:
        return cached
    
    print("🔄 Загружаю список пар с Bybit...")
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
                if pairs:
                    save_symbols_cache(pairs)
                    print(f"✅ Получено {len(pairs)} пар")
                    return pairs
    except Exception as e:
        print(f"⚠️ Ошибка: {e}")
    
    print("⚠️ Использую базовый список")
    return ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT']

# ==================== ЗАГРУЗКА ИСТОРИИ ====================
def fetch_klines(symbol):
    try:
        resp = requests.get("https://api.bybit.com/v5/market/kline", params={"category": "linear", "symbol": symbol, "interval": "15", "limit": 100}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('retCode') == 0:
                klines = data['result']['list']
                klines.reverse()
                return klines
    except:
        pass
    return None

def init_symbol_data(symbols):
    global symbol_buffers, symbol_volume_buffers
    
    try:
        if os.path.exists("history_cache_email.pkl"):
            with open("history_cache_email.pkl", "rb") as f:
                cache = pickle.load(f)
                if cache and 'buffers' in cache:
                    symbol_buffers = cache['buffers']
                    symbol_volume_buffers = cache['volume_buffers']
                    print(f"⚡ Загружено {len(symbol_buffers)} пар из кэша истории (МГНОВЕННО!)")
                    return
    except Exception as e:
        print(f"⚠️ Ошибка загрузки кэша истории: {e}")
    
    print(f"⏳ Начинаю загрузку {len(symbols)} пар...")
    for idx, sym in enumerate(symbols, 1):
        if idx % 50 == 0:
            print(f"📊 Прогресс: {idx}/{len(symbols)}")
        klines = fetch_klines(sym)
        if klines:
            closes = [float(k[4]) for k in klines]
            symbol_buffers[sym] = deque(closes, maxlen=200)
            turnovers = [float(k[6]) for k in klines[-96:]]
            symbol_volume_buffers[sym] = deque(turnovers, maxlen=96)
        time.sleep(0.02)
    
    try:
        cache = {
            'symbols': list(symbol_buffers.keys()),
            'buffers': symbol_buffers,
            'volume_buffers': symbol_volume_buffers
        }
        with open("history_cache_email.pkl", "wb") as f:
            pickle.dump(cache, f)
        print(f"💾 Кэш истории сохранён: {len(symbol_buffers)} пар")
    except Exception as e:
        print(f"⚠️ Ошибка сохранения кэша истории: {e}")
    
    print(f"✅ Загружено {len(symbol_buffers)} пар")

# ==================== ИНДИКАТОРЫ ====================
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

# ==================== МГНОВЕННАЯ ПРОВЕРКА ====================
def check_signal_instant(symbol, current_price, bb_high, bb_low, volume_24h):
    try:
        short_cond = current_price >= (bb_high * (1 + BB_BUFFER_PCT / 100))
        long_cond = current_price <= (bb_low * (1 - BB_BUFFER_PCT / 100))
        
        print(f"🔍 {symbol}: цена={current_price:.6f}, bb_high={bb_high:.6f}, bb_low={bb_low:.6f}, объём={volume_24h:,.0f}", flush=True)
        print(f"   SHORT: {short_cond} (нужно >= {bb_high * (1 + BB_BUFFER_PCT / 100):.6f})", flush=True)
        print(f"   LONG:  {long_cond} (нужно <= {bb_low * (1 - BB_BUFFER_PCT / 100):.6f})", flush=True)
        
        if not short_cond and not long_cond:
            print(f"⏩ {symbol}: условия не выполнены", flush=True)
            return None
        
        closes = list(symbol_buffers.get(symbol, []))
        rsi = calculate_rsi(closes) if len(closes) > RSI_PERIOD else 50
        print(f"   RSI: {rsi:.1f}", flush=True)
        
        if RSI_OVERBOUGHT > 0 and short_cond and rsi < RSI_OVERBOUGHT:
            print(f"⏩ {symbol}: RSI {rsi:.1f} < {RSI_OVERBOUGHT} (нужно для SHORT)", flush=True)
            return None
        if RSI_OVERSOLD > 0 and long_cond and rsi > RSI_OVERSOLD:
            print(f"⏩ {symbol}: RSI {rsi:.1f} > {RSI_OVERSOLD} (нужно для LONG)", flush=True)
            return None
        
        signal = None
        if short_cond:
            deviation = ((current_price / bb_high) - 1) * 100
            signal = {
                'side': 'SHORT',
                'symbol': symbol,
                'price': current_price,
                'bb_high': bb_high,
                'bb_low': bb_low,
                'deviation': deviation,
                'volume_24h': volume_24h,
                'rsi': rsi,
                'candle_time': datetime.now().strftime('%H:%M:%S')
            }
            print(f"⚡ СИГНАЛ SHORT {symbol} (откл: {deviation:.2f}%)", flush=True)
        elif long_cond:
            deviation = ((bb_low / current_price) - 1) * 100
            signal = {
                'side': 'LONG',
                'symbol': symbol,
                'price': current_price,
                'bb_high': bb_high,
                'bb_low': bb_low,
                'deviation': deviation,
                'volume_24h': volume_24h,
                'rsi': rsi,
                'candle_time': datetime.now().strftime('%H:%M:%S')
            }
            print(f"⚡ СИГНАЛ LONG {symbol} (откл: {deviation:.2f}%)", flush=True)
        
        if signal:
            stats['total_signals'] += 1
            send_email_signal(signal)
            return signal
        return None
    except Exception as e:
        print(f"⚠️ Ошибка в check_signal_instant: {e}")
        return None

async def fetch_volumes():
    while True:
        try:
            resp = requests.get("https://api.bybit.com/v5/market/tickers", params={"category": "linear"}, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('retCode') == 0:
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
            
            print(f"\n🔄 Проверка {len(symbols)} пар...", flush=True)
            
            # ==================== ПРИНУДИТЕЛЬНАЯ ПРОВЕРКА BTCUSDT ====================
            # Проверяем BTCUSDT в первую очередь (для быстрого теста)
            try:
                resp = requests.get("https://api.bybit.com/v5/market/tickers", params={"category": "linear", "symbol": "BTCUSDT"}, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('retCode') == 0:
                        tickers = data['result']['list']
                        if tickers:
                            current_price = float(tickers[0].get('lastPrice', 0))
                            volume_24h = live_volumes.get("BTCUSDT", 0)
                            bb = bollinger_cache.get("BTCUSDT")
                            if bb:
                                bb_high, bb_low = bb
                                print(f"\n🔥 ПРИНУДИТЕЛЬНАЯ ПРОВЕРКА BTCUSDT:", flush=True)
                                check_signal_instant("BTCUSDT", current_price, bb_high, bb_low, volume_24h)
            except Exception as e:
                print(f"⚠️ Ошибка принудительной проверки BTCUSDT: {e}", flush=True)
            # ======================================================================
            
            for symbol in symbols:
                try:
                    resp = requests.get("https://api.bybit.com/v5/market/tickers", params={"category": "linear", "symbol": symbol}, timeout=5)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get('retCode') == 0:
                            tickers = data['result']['list']
                            if tickers:
                                current_price = float(tickers[0].get('lastPrice', 0))
                                volume_24h = live_volumes.get(symbol, 0)
                                
                                bb = bollinger_cache.get(symbol)
                                if bb:
                                    bb_high, bb_low = bb
                                    check_signal_instant(symbol, current_price, bb_high, bb_low, volume_24h)
                            else:
                                print(f"⚠️ {symbol}: нет тикеров", flush=True)
                        else:
                            print(f"⚠️ {symbol}: ошибка API - {data.get('retMsg')}", flush=True)
                    else:
                        print(f"⚠️ {symbol}: HTTP {resp.status_code}", flush=True)
                except Exception as e:
                    print(f"⚠️ Ошибка при проверке {symbol}: {e}", flush=True)
                await asyncio.sleep(0.02)
            
            print(f"✅ Проверка {len(symbols)} пар завершена\n", flush=True)
            await asyncio.sleep(30)
            
        except Exception as e:
            print(f"⚠️ Ошибка: {e}")
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
            
            print(f"📐 Полосы рассчитаны для {len(bollinger_cache)} пар")
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
    print("🤖 БОТ-СКАНЕР BOLLINGER + RSI (МГНОВЕННЫЕ СИГНАЛЫ + ЛОГИ)")
    print("=" * 60)
    print(f"📊 НАСТРОЙКИ:")
    print(f"   Буфер: {BB_BUFFER_PCT}%")
    print(f"   RSI: {RSI_OVERBOUGHT}/{RSI_OVERSOLD}")
    print(f"   Объём: > {VOLUME_24H_THRESHOLD:,.0f} USDT")
    print("=" * 60)
    
    send_email_message("🚀 БОТ ЗАПУЩЕН! (МГНОВЕННЫЕ СИГНАЛЫ + ЛОГИ)")
    
    threading.Thread(target=stats_printer, daemon=True).start()
    
    symbols = get_all_usdt_futures()
    if not symbols:
        send_email_message("❌ Нет пар")
        return
    
    init_symbol_data(symbols)
    send_email_message(f"✅ Загружено {len(symbol_buffers)} пар")
    
    asyncio.create_task(fetch_volumes())
    asyncio.create_task(monitor_active_pairs())
    await check_symbols()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлен")
