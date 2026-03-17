import asyncio
import websockets
import json
import logging
import aiohttp
import time
import os
from dotenv import load_dotenv

# Carrega as variáveis do ficheiro .env
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class CryptoSentinel:
    def __init__(self):
        self.prices = {"binance": 0.0, "kraken": 0.0}
        self.active = True
        
        # Configurações do Telegram
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        # O "Calcanhar de Aquiles" do n8n: Controlo de Estado em Memória
        self.last_alert_time = 0
        self.cooldown_seconds = 10 # Só envia 1 alerta a cada 10 segundos

    async def connect_binance(self):
        url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
        async with websockets.connect(url) as ws:
            logger.info("✅ Conectado à Binance")
            while self.active:
                msg = await ws.recv()
                data = json.loads(msg)
                self.prices["binance"] = float(data['p'])
                self.check_arbitrage()

    async def connect_kraken(self):
        url = "wss://ws.kraken.com/"
        async with websockets.connect(url) as ws:
            subscribe_msg = {
                "event": "subscribe",
                "pair": ["BTC/USD"],
                "subscription": {"name": "ticker"}
            }
            await ws.send(json.dumps(subscribe_msg))
            logger.info("✅ Conectado à Kraken")

            while self.active:
                msg = await ws.recv()
                data = json.loads(msg)
                if isinstance(data, list):
                    self.prices["kraken"] = float(data[1]['a'][0])
                    self.check_arbitrage()

    async def send_telegram_alert(self, message):
        """Envia o alerta de forma assíncrona sem bloquear os websockets"""
        if not self.bot_token or not self.chat_id:
            logger.warning("Credenciais do Telegram ausentes. Alerta ignorado.")
            return

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.info("📩 Alerta Telegram enviado com sucesso!")
                    else:
                        logger.error(f"Erro no Telegram: API retornou {response.status}")
        except Exception as e:
            logger.error(f"Erro de rede ao enviar alerta: {e}")

    def check_arbitrage(self):
        b = self.prices["binance"]
        k = self.prices["kraken"]

        if b > 0 and k > 0:
            diff = abs(b - k)
            spread_perc = (diff / max(b, k)) * 100
            
            # Vamos supor que uma diferença de 0.1% já é lucro real
            if spread_perc > 0.1: 
                current_time = time.time()
                
                # Validação de Cooldown ultra-rápida (Microssegundos)
                if current_time - self.last_alert_time > self.cooldown_seconds:
                    msg = (
                        f"🚨 <b>OPORTUNIDADE DE ARBITRAGEM</b> 🚨\n\n"
                        f"📈 <b>Spread:</b> {spread_perc:.3f}%\n"
                        f"💰 <b>Diferença Bruta:</b> ${diff:.2f}\n"
                        f"🔸 Binance: ${b:.2f}\n"
                        f"🦑 Kraken: ${k:.2f}"
                    )
                    
                    logger.info(f"💰 Lucro detetado! Spread: {spread_perc:.3f}% | Dif: ${diff:.2f}")
                    
                    # Dispara o envio HTTP em background ("Fire and Forget")
                    # O código NÃO para de processar os preços enquanto o envio acontece!
                    asyncio.create_task(self.send_telegram_alert(msg))
                    
                    self.last_alert_time = current_time

    async def run(self):
        await asyncio.gather(
            self.connect_binance(),
            self.connect_kraken()
        )

if __name__ == "__main__":
    sentinel = CryptoSentinel()
    try:
        asyncio.run(sentinel.run())
    except KeyboardInterrupt:
        logger.info("🛑 Sentinel desligado com segurança.")