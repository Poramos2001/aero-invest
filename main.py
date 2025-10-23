import json

def main():
    print("\033[91mThis is red text\033[0m")
    print("\033[92mSuccess!\033[0m")
    print("\033[93mWarning!\033[0m")
    print("\033[94mInfo message\033[0m")
    with open("companies.json", "r") as f:
        companies = json.load(f)
    
    print(companies)


if __name__ == "__main__":
    main()
