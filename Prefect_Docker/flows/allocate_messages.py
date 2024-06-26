from prefect import flow, task, get_run_logger
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_email import EmailServerCredentials, email_send_message

# Load blocks
email_server_credentials = EmailServerCredentials.load("websmtp")


@task
def fetch_data():
    db_conn = SqlAlchemyConnector.load("sqlalchemy")
    SqlAlchemyConnector
    results = db_conn.fetch_many(
        "SELECT * FROM success_messages WHERE msg_id = :id;", parameters={"id": "1"}
    )
    return results


@task
def insert_message(table, message):
    db_conn = SqlAlchemyConnector.load("sqlalchemy")
    db_conn.execute(
        f"INSERT INTO {table} (msg_id, msg_date, msg_time, message) VALUES( :id, :msg_date, :msg_time, :message ) ON CONFLICT DO NOTHING",
        parameters={
            "id": f"{message[0]}",
            "msg_date": message[1],
            "msg_time": message[2],
            "message": f"{message[3]}",
        },
    )


@task
def insert_message_with_category(table, message, category):
    db_conn = SqlAlchemyConnector.load("sqlalchemy")
    print(message)

    # db_conn.execute(
    #     f"INSERT INTO {table} (msg_id, msg_date, msg_time, message, category) VALUES( %s, %s, %s, %s, %s ) ON CONFLICT DO NOTHING",
    #     (message[0], message[1], message[2], message[3], category),
    # )

    db_conn.execute(
        f"INSERT INTO {table} (msg_id, msg_date, msg_time, message, category) VALUES( :id, :msg_date, :msg_time, :message, :category ) ON CONFLICT DO NOTHING",
        parameters={
            "id": f"{message[0]}",
            "msg_date": message[1],
            "msg_time": message[2],
            "message": f"{message[3]}",
            "category": f"{category}",
        },
    )


@flow
def process_error_messages():
    db_conn = SqlAlchemyConnector.load("sqlalchemy")

    print("proccess error messages")
    message_ids = []
    message_html = ""
    messages = db_conn.fetch_many("select * from messages where msg_type = 'error'")
    for message in messages:
        insert_message("error_messages", message)
        message_ids.append(message[0])
        message_html += f"<p> {message[1]} / {message[2]}: {message[3]}</p>"
        print(f"insert errr ID: {message[0]}")

    if message_html != "":
        email_send_message(
            email_server_credentials=email_server_credentials,
            subject="Error Message in Log",
            msg=f""" Hi, <br>
                        <p>Following messages were raised in System</p>
                        {message_html}
                        <br> Thank You. <br>
                    """,
            email_to="daniel.stuckenberger@studmail.htw-aalen.de",
        )

    return message_ids

@flow
def process_warning_messages():
    db_conn = SqlAlchemyConnector.load("sqlalchemy")

    print("proccess warning messages")
    message_ids = []
    category = ""

    messages = db_conn.fetch_many("select * from messages where msg_type = 'warning'")

    for message in messages:
        text = message[3]
        if "[GLOBAL]" in text:
            category = "global"
        elif "[SPECIAL]" in text:
            category = "special"
        else:
            category = "unknown"

        insert_message_with_category("warning_messages", message, category)
        message_ids.append(message[0])
        print(f"insert warning ID: {message[0]}")

    return message_ids


@flow
def process_success_messages():
    db_conn = SqlAlchemyConnector.load("sqlalchemy")

    print("proccess success messages")
    message_ids = []
    messages = db_conn.fetch_many("select * from messages where msg_type = 'success'")
    print(messages)

    for message in messages:
        insert_message("success_messages", message)
        message_ids.append(message[0])
        print(f"insert success ID: {message[0]}")

    return message_ids

@flow
def process_unknown_messages():
    logger = get_run_logger()
    db_conn = SqlAlchemyConnector.load("sqlalchemy")
    print("proccess unknown messages")
    message_ids = []
    messages = db_conn.fetch_many(
        "select * from messages where msg_type != 'success' and msg_type != 'warning' and msg_type != 'error'"
    )
    for message in messages:
        logger.error(f"Message ID {message[0]} has the wrong Type: {message[4]}")
        message_ids.append(message[0])

    return message_ids



@flow
def delete_original_messages(message_ids):
    print("delete original messages")

    for id in message_ids:
        db_conn = SqlAlchemyConnector.load("sqlalchemy")
        db_conn.execute("DELETE FROM messages WHERE msg_id = '{}'".format(id))


@flow
def allocate_messages():
    message_ids = []
    message_ids += process_success_messages()
    message_ids += process_warning_messages()
    message_ids += process_error_messages()
    message_ids += process_unknown_messages()

    delete_original_messages(message_ids)


if __name__ == "__main__":
    allocate_messages()
