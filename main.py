from single_redis import SingleRedisDumper, SingleRedisPumper

if __name__ == "__main__":
    SingleRedisDumper('redis://localhost:6379').dump_all()
    SingleRedisPumper('redis://localhost:6379/1').pump_with_pattern('*')
