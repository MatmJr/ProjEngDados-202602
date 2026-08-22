from src.extract import Extract
from src.load import Load


def main():
    ext = Extract()
    data = ext.pnadc(variavel=4099, estado=26)

    ld = Load()
    ld.load_mongo(data, "IBGE", "PNADC")


if __name__ == "__main__":
    main()
