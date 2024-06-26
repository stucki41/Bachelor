import pandas as pd
import asyncio
from tensorflow import keras
from tensorflow.keras import layers
from prefect import flow, task, runtime, get_run_logger


@task
def load_dataset():
    # from IPython.display import display
    red_wine = pd.read_csv("~/flows/data/dataset/winequality-red.csv")
    # Create training and validation splits
    df_train = red_wine.sample(frac=0.7, random_state=0)
    df_valid = red_wine.drop(
        df_train.index
    )  # drop from red_wine the lines (indices) contained in df_train

    # Scale to [0, 1]
    max_ = df_train.max(axis=0)
    min_ = df_train.min(axis=0)
    df_train = (df_train - min_) / (max_ - min_)
    df_valid = (df_valid - min_) / (max_ - min_)
    df_train.to_csv(f"~/flows/data/train/wq-scaled.csv", index=False)
    df_valid.to_csv(f"~/flows/data/valid/wq-scaled.csv", index=False)


@task
def split_dataset():
    # Split features and target
    df_train = pd.read_csv(f"~/flows/data/train/wq-scaled.csv")
    df_valid = pd.read_csv(f"~/flows/data/valid/wq-scaled.csv")

    X_train = df_train.drop("quality", axis=1)
    X_valid = df_valid.drop("quality", axis=1)
    y_train = df_train["quality"]
    y_valid = df_valid["quality"]

    X_train.to_csv(f"~/flows/data/train/X_train.csv", index=False)
    y_train.to_csv(f"~/flows/data/train/y_train.csv", index=False)
    X_valid.to_csv(f"~/flows/data/valid/X_valid.csv", index=False)
    y_valid.to_csv(f"~/flows/data/valid/y_valid.csv", index=False)


@task
def train_model(run_id):
    model = keras.Sequential(
        [
            layers.Dense(512, activation="relu", input_shape=[11]),
            layers.Dense(512, activation="relu"),
            layers.Dense(512, activation="relu"),
            layers.Dense(1),
        ]
    )
    model.compile(
        optimizer="adam",
        loss="mae",
    )

    X_train = pd.read_csv(f"~/flows/data/train/X_train.csv")
    y_train = pd.read_csv(f"~/flows/data/train/y_train.csv")
    X_valid = pd.read_csv(f"~/flows/data/valid/X_valid.csv")
    y_valid = pd.read_csv(f"~/flows/data/valid/y_valid.csv")

    history = model.fit(
        X_train,
        y_train,
        validation_data=(X_valid, y_valid),
        batch_size=256,
        epochs=1000,
    )
    # history_df = pd.DataFrame(history.history)
    # print(history_df)
    # return model
    model.save("data/models/{}.keras".format(run_id))


@task
def predict_value_on_test_data(run_id):
    import tensorflow as tf

    logger = get_run_logger()
    df_valid = pd.read_csv(f"~/flows/data/valid/wq-scaled.csv")

    model = tf.keras.models.load_model("data/models/{}.keras".format(run_id))
    expl = df_valid.sample(n=1)
    X = expl.drop("quality", axis=1)
    y = expl["quality"]
    yp = model.predict(X)

    logger.info(f"expected: {y} / predicted: {yp}")


@flow
async def process_dataset():
    run_id = runtime.flow_run.name
    print(run_id)
    load_dataset()
    split_dataset()
    train_model(run_id)
    predict_value_on_test_data(run_id)

@flow
async def main_flow():
    parallel_subflows = [process_dataset(),process_dataset(),process_dataset(),process_dataset(),process_dataset()]
    await asyncio.gather(*parallel_subflows)
    # for _ in range(5):
    #     await process_dataset()


if __name__ == "__main__":
    main_flow_state = asyncio.run(main_flow())
    
