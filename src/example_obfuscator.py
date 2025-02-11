import argparse
import json
from obfuscator import obfuscator

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script that obfuscates the mentioned fields in the passed s3 object file"
    )
    parser.add_argument("--s3FilePath", type=str)
    parser.add_argument("--obfuscateFields", nargs="+", type=str)
    args = parser.parse_args()

    d = {}
    d["file_to_obfuscate"], d["pii_fields"] = args.s3FilePath, args.obfuscateFields
    json_str = json.dumps(d)

    mod_s3_obj = obfuscator(json_str).decode()
    print(mod_s3_obj)
