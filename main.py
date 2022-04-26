from single_redis import RedisDumper

if __name__ == "__main__":
    RedisDumper('redis://localhost:6379').dump_all()
