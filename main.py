from single_redis import RedisDumper, RedisPumper

if __name__ == "__main__":
    RedisDumper('redis://localhost:6379').dump_all()
    RedisPumper('redis://localhost:6379/1').pump_with_pattern('*')
