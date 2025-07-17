import logging
import time
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, List
from queue import Queue, Empty
import signal

from coinex_api import CoinExAPI
from market_analyzer import MarketAnalyzer
from trade_manager import TradeManager
from telegram_manager import TelegramManager
from config_manager import ConfigManager
from health_check import HealthCheck
from event_dispatcher import EventDispatcher

# تنظیمات لاگ‌گیری ساختاریافته
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SmartMoneyBot")

class SmartMoneyBot:
    """
    کلاس اصلی ربات معاملاتی هوشمند با قابلیت‌های:
    - تحلیل چندزمانی بازار
    - مدیریت خودکار معاملات
    - پشتیبانی از دستورات تلگرام
    - مانیتورینگ لحظه‌ای
    """
    
    def __init__(self):
        # بارگذاری تنظیمات
        self.config = ConfigManager()
        
        # راه‌اندازی ماژول‌های اصلی
        self.coinex = CoinexAPI(
            api_key=self.config.api_key,
            api_secret=self.config.api_secret,
            rate_limit=self.config.api_rate_limit
        )
        
        self.analyzer = MarketAnalyzer(
            config=self.config.analysis_config,
            data_cache=self.config.data_cache
        )
        
        self.trade_manager = TradeManager(
            exchange=self.coinex,
            config=self.config.trading_config,
            risk_manager=self.config.risk_manager
        )
        
        self.telegram = TelegramManager(
            token=self.config.telegram_token,
            chat_id=self.config.telegram_chat_id,
            auth_token=self.config.telegram_auth_token
        )
        
        # سیستم‌های کمکی
        self.health_check = HealthCheck(self)
        self.event_dispatcher = EventDispatcher()
        self.worker_pool = ThreadPoolExecutor(max_workers=5)
        
        # مدیریت وضعیت
        self.running = False
        self.command_queue = Queue()
        
        # ثبت رویدادها
        self._register_events()
        
    def _register_events(self):
        """ثبت رویدادهای سیستمی"""
        self.event_dispatcher.register(
            'market_data_updated',
            self._handle_market_data_update
        )
        self.event_dispatcher.register(
            'trade_executed',
            self._handle_trade_execution
        )
        self.event_dispatcher.register(
            'error_occurred',
            self._handle_error
        )
        
    def start(self):
        """شروع عملیات ربات"""
        try:
            self.running = True
            logger.info("Starting Smart Money Trading Bot")
            
            # راه‌اندازی سیستم‌های جانبی
            self.health_check.start()
            self.telegram.start(self._handle_telegram_command)
            
            # ثبت handler برای خروج ایمن
            signal.signal(signal.SIGINT, self._graceful_shutdown)
            signal.signal(signal.SIGTERM, self._graceful_shutdown)
            
            # حلقه اصلی
            while self.running:
                try:
                    self._main_loop()
                except Exception as e:
                    logger.error(f"Error in main loop: {str(e)}", exc_info=True)
                    self._handle_error(e)
                    
        except Exception as e:
            logger.critical(f"Fatal error: {str(e)}", exc_info=True)
            self._emergency_shutdown(e)
            
    def _main_loop(self):
        """حلقه اصلی پردازش"""
        start_time = datetime.utcnow()
        
        # بررسی سلامت سیستم
        if not self.health_check.is_healthy():
            logger.warning("System health check failed. Waiting...")
            time.sleep(60)
            return
            
        # پردازش دستورات دریافتی
        self._process_commands()
        
        # بررسی زمان‌بندی تحلیل بازار
        if self._should_analyze_market(start_time):
            self.worker_pool.submit(self._run_analysis_cycle)
            
        # مدیریت معاملات باز
        self.worker_pool.submit(self.trade_manager.monitor_open_trades)
        
        # استراحت بین چرخه‌ها
        time.sleep(self.config.loop_interval)
        
    def _should_analyze_market(self, current_time: datetime) -> bool:
        """تعیین زمان مناسب برای تحلیل بازار"""
        # تحلیل فقط در ساعات فعال بازار
        if not self._is_market_open(current_time):
            return False
            
        # تحلیل بر اساس بازه‌های زمانی از پیش تعیین شده
        analysis_intervals = self.config.analysis_intervals
        for interval in analysis_intervals:
            if current_time.minute % interval == 0:
                return True
        return False
        
    def _is_market_open(self, dt_utc: datetime) -> bool:
        """بررسی باز بودن بازارهای جهانی"""
        # لندن: 7:00 - 16:00 UTC
        # نیویورک: 13:00 - 22:00 UTC
        london_open = dt_utc.replace(hour=7, minute=0, second=0, microsecond=0)
        london_close = dt_utc.replace(hour=16, minute=0, second=0, microsecond=0)
        ny_open = dt_utc.replace(hour=13, minute=0, second=0, microsecond=0)
        ny_close = dt_utc.replace(hour=22, minute=0, second=0, microsecond=0)
        
        return (london_open <= dt_utc <= london_close) or (ny_open <= dt_utc <= ny_close)
        
    def _run_analysis_cycle(self):
        """چرخه کامل تحلیل و معامله"""
        try:
            logger.info("Starting market analysis cycle")
            
            # دریافت داده‌های بازار
            market_data = self.coinex.get_market_data()
            if not market_data:
                raise ValueError("No market data received")
                
            # تحلیل داده‌ها
            analysis_result = self.analyzer.analyze(market_data)
            
            # مدیریت معاملات بر اساس تحلیل
            trade_decisions = self.trade_manager.make_decisions(analysis_result)
            
            # ارسال نتایج
            self._send_analysis_report(analysis_result, trade_decisions)
            
        except Exception as e:
            self.event_dispatcher.dispatch('error_occurred', e)
            
    def _process_commands(self):
        """پردازش دستورات دریافتی"""
        try:
            while True:
                try:
                    command = self.command_queue.get_nowait()
                    self._execute_command(command)
                except Empty:
                    break
        except Exception as e:
            logger.error(f"Error processing commands: {str(e)}")
            
    def _execute_command(self, command: Dict):
        """اجرای یک دستور خاص"""
        cmd = command.get('text', '').lower()
        
        if cmd == '/status':
            status = self._generate_status_report()
            self.telegram.send_message(status)
        elif cmd == '/stop':
            self._graceful_shutdown()
        elif cmd == '/trades':
            trades = self.trade_manager.get_trades_report()
            self.telegram.send_message(trades)
        else:
            self.telegram.send_message("Unknown command. Try /status")
            
    def _handle_telegram_command(self, message: Dict):
        """مدیریت دستورات تلگرام"""
        self.command_queue.put(message)
        
    def _handle_market_data_update(self, data: Dict):
        """مدیریت به‌روزرسانی داده‌های بازار"""
        logger.info(f"Market data updated: {data['symbol']}")
        
    def _handle_trade_execution(self, trade: Dict):
        """مدیریت اجرای معاملات"""
        logger.info(f"Trade executed: {trade}")
        self.telegram.send_message(
            f"Trade executed:\n"
            f"Symbol: {trade['symbol']}\n"
            f"Type: {trade['type']}\n"
            f"Price: {trade['price']}\n"
            f"Amount: {trade['amount']}"
        )
        
    def _handle_error(self, error: Exception):
        """مدیریت خطاهای سیستم"""
        logger.error(f"System error: {str(error)}")
        self.telegram.send_message(f"⚠️ System error: {str(error)}")
        
        if isinstance(error, (ConnectionError, TimeoutError)):
            self._handle_connection_error()
            
    def _handle_connection_error(self):
        """مدیریت خطاهای اتصال"""
        logger.warning("Connection error detected. Waiting to reconnect...")
        time.sleep(60)
        
    def _graceful_shutdown(self, signum=None, frame=None):
        """خروج ایمن از برنامه"""
        logger.info("Initiating graceful shutdown...")
        self.running = False
        
        # توقف سیستم‌های جانبی
        self.worker_pool.shutdown(wait=True)
        self.health_check.stop()
        self.telegram.stop()
        
        logger.info("Bot shutdown completed")
        sys.exit(0)
        
    def _emergency_shutdown(self, error: Exception):
        """خروج اضطراری"""
        logger.critical("EMERGENCY SHUTDOWN INITIATED")
        try:
            self.telegram.send_message(
                f"🚨 EMERGENCY SHUTDOWN\n"
                f"Reason: {str(error)}"
            )
        except:
            pass
            
        self._graceful_shutdown()
        
    def _send_analysis_report(self, analysis: Dict, trades: List[Dict]):
        """ارسال گزارش تحلیل به تلگرام"""
        report = (
            "📊 Market Analysis Report\n"
            f"Time: {datetime.utcnow()}\n"
            f"Trend: {analysis['trend']}\n"
            f"Key Levels: {analysis['key_levels']}\n"
            f"Trade Signals: {len(trades)}"
        )
        self.telegram.send_message(report)
        
    def _generate_status_report(self) -> str:
        """تولید گزارش وضعیت سیستم"""
        health = self.health_check.get_status()
        trades = self.trade_manager.get_trades_summary()
        
        return (
            "🤖 Bot Status\n"
            f"Uptime: {health['uptime']}\n"
            f"CPU: {health['cpu']}%\n"
            f"Memory: {health['memory']}%\n"
            f"Open Trades: {trades['open']}\n"
            f"Closed Trades: {trades['closed']}\n"
            f"Balance: {trades['balance']}"
        )

if __name__ == "__main__":
    try:
        bot = SmartMoneyBot()
        bot.start()
    except Exception as e:
        logging.critical(f"Failed to start bot: {str(e)}", exc_info=True)
        sys.exit(1)
