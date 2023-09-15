import logging

class LogUtil:
    logger = None
    @staticmethod
    def init_log():
        """
        初始化日志配置，只需要初始化一次
        """
        if not LogUtil.logger:
            # 创建日志器
            LogUtil.logger = logging.getLogger('main')
            LogUtil.logger.setLevel(logging.DEBUG)
            # 创建日志处理器，将日志记录输出到控制台
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            # 创建日志格式化器
            console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            # 将日志格式化器添加到处理器中
            console_handler.setFormatter(console_formatter)
            # 将处理器添加到日志器中
            LogUtil.logger.addHandler(console_handler)