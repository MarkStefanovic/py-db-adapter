import sqlalchemy as sa

metadata = sa.MetaData()

users = sa.Table(
    "users",
    metadata,
    sa.Column("id", sa.Integer, sa.Sequence("user_id"), primary_key=True),
    sa.Column("name", sa.String),
    sa.Column("fullname", sa.String),
)

addresses = sa.Table(
    "addresses",
    metadata,
    sa.Column("id", sa.Integer, sa.Sequence("address_id_seq"), primary_key=True),
    sa.Column("user_id", None, sa.ForeignKey("users.id")),
    sa.Column("email_address", sa.String, nullable=False),
)

balances = sa.Table(
    "balances",
    metadata,
    sa.Column("id", sa.Integer, sa.Sequence("balances_id_seq"), primary_key=True),
    sa.Column("user_id", None, sa.ForeignKey("users.id")),
    sa.Column("balance", sa.DECIMAL(19, 2), nullable=False),
)


if __name__ == "__main__":
    engine = sa.engine.create_engine("sqlite://", echo=True)
    metadata.create_all(engine)

    ins = users.insert().values(name="Jack", fullname="Jack Jones")
    print(str(ins))
    print(ins.compile().params)

    with engine.connect() as con:
        result = con.execute(ins)
        pk = result.inserted_primary_key
        print(f"{pk=}")

        # the following is the more typical way inserts are done using the expression language
        con.execute(
            users.insert(), {"id": 2, "name": "wendy", "fullname": "Wendy Williams"}
        )
        con.execute(
            addresses.insert(),
            [
                {"user_id": 1, "email_address": "jack@yahoo.com"},
                {"user_id": 1, "email_address": "jack@msn.com"},
                {"user_id": 2, "email_address": "www@www.org"},
                {"user_id": 2, "email_address": "wendy@aol.com"},
            ],
        )
