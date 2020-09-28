from database import MongoDatabase

if __name__ == '__main__':
    database = MongoDatabase("nonprod")

    balance_sheets = database.find_one("balance_sheets", {})

    for balance_sheet in balance_sheets:
        print(balance_sheet)
