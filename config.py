# config.py
CONFIG = {
    'nrf24-channel': 0x76,
    'nrf24-pipes': [[0xe7, 0xe7, 0xe7, 0xe7, 0xe7], [0xc2, 0xc2, 0xc2, 0xc2, 0xc2]],
    'csn_pin': 0,
    'ce_pin': 22,
    'rx_hold_timeout': 30,
    'kafka_producer': '192.168.0.19:9092',
    'kafka_channel': 'octonade-pipe',
    'error_file': 'errors/file.log'
}
