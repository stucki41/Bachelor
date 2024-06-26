from prefect import flow, task,  get_run_logger

@task
def say_hello(name):
    print(f"hello {name}")

@task
def say_goodbye(name):
    print(f"goodbye {name}")



@flow(name="test flow",log_prints=True)
def greetings(names=["arthur", "trillian"]):
    logger =  get_run_logger()
    logger.info("This is a log message")
    logger.warning("This is a Warning")
    logger.error("This is an Error")
    logger.critical("Critical Error")
    for name in names:
        say_hello(name)
        say_goodbye(name)
    
    





if __name__ == "__main__":
    greetings()
