import logging


def setup_logging(level=logging.DEBUG, use_file: bool = False):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    handlers = [console_handler]

    if use_file:
        file_handler = logging.FileHandler('tests.log')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        handlers.append(file_handler)

    logging.basicConfig(level=level, handlers=handlers)


setup_logging(logging.ERROR)
