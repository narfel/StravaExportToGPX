#!/usr/bin/env python3

import argparse
import csv
import fileinput
import gzip
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime
from typing import Dict, IO, List, Optional


def date_format(text: str) -> datetime:
    for fmt in ("%b %d, %Y, %I:%M:%S %p",):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError("No valid date format found")


def matches_filter_types(activity: Dict, filter_types: Optional[List]) -> bool:
    if not filter_types:
        return True
    activity_type = activity["type"].lower()
    for filter_type in filter_types:
        if filter_type.lower() == activity_type:
            return True
    return False


def matches_filter_years(activity_date: Dict, filter_years: Optional[List]) -> bool:
    if not filter_years:
        return True
    activity_year = activity_date["date"][0:4]
    if activity_year in filter_years:
        return True
    return False


GPSBABEL_FILE_TYPE = {"fit": "garmin_fit", "tcx": "gtrnctr"}


def gpsbabel_convert(input_file_path: str, output_file_path: str, file_type: str):
    subprocess.run(
        [
            "gpsbabel",
            "-i",
            GPSBABEL_FILE_TYPE[file_type],
            "-f",
            input_file_path,
            "-o",
            "gpx",
            "-F",
            output_file_path,
        ]
    )


def gunzip(gzip_file_name: str, target_file_obj: IO):
    with gzip.open(gzip_file_name, "rb") as gzip_file:
        shutil.copyfileobj(gzip_file, target_file_obj)
        target_file_obj.flush()


def zip_extract(zip_file: zipfile.ZipFile, file_name: str, target_file_obj: IO):
    with zip_file.open(file_name) as fp:
        shutil.copyfileobj(fp, target_file_obj)
        target_file_obj.flush()


def clear_temp(args: str, stray_file: str):
    if os.path.exists(stray_file):
        try:
            if args:
                print(f"Removing tempfile: {stray_file}")
            os.unlink(stray_file)
        except Exception as exception_msg:
            print(f"Error while deleting file {exception_msg}")


def convert_activity(activity_file_name: str, target_gpx_file_name: str):
    if (
        activity_file_name.endswith(".fit.gz")
        or activity_file_name.endswith(".tcx.gz")
        or activity_file_name.endswith(".gpx.gz")
    ):
        suffix = activity_file_name[-7:-3]
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as gunzipped_file:
            with gzip.open(activity_file_name, "rb") as gzip_file:
                shutil.copyfileobj(gzip_file, gunzipped_file)
                gunzipped_file.flush()
            convert_activity(gunzipped_file.name, target_gpx_file_name)
            gunzipped_file.close()
            os.unlink(gunzipped_file.name)

    elif activity_file_name.endswith(".fit"):
        gpsbabel_convert(activity_file_name, target_gpx_file_name, "fit")

    elif activity_file_name.endswith(".tcx"):
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            with open(activity_file_name, "r") as file_to_strip:
                for line in file_to_strip:
                    stripped_line = line.strip() + "\n"
                    temp.write(stripped_line.encode("utf-8"))
            temp.close()
            gpsbabel_convert(temp.name, target_gpx_file_name, "tcx")
            os.unlink(temp.name)

    elif activity_file_name.endswith(".gpx"):
        shutil.copyfile(activity_file_name, target_gpx_file_name)

    else:
        print(f"Unrecognized/unsupported file format: {activity_file_name}\n")


def print_usage_error(args_parser: argparse.ArgumentParser, message: str):
    args_parser.print_usage()
    sys.stderr.write(message)
    sys.exit(2)


def get_activities(
    zip_file: Optional[zipfile.ZipFile], csv_file_name: str
) -> List[Dict]:
    if zip_file:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as unzipped_file:
            zip_extract(zip_file, csv_file_name, unzipped_file)
            return get_activities(None, unzipped_file.name)
    with open(csv_file_name, encoding="utf8") as csv_file:
        activities = list(csv.DictReader(csv_file))
        if len(activities) == 0:
            return []

        keys = list(activities[0].keys())
        if len(keys) != 68:
            raise Exception(
                f"Unexpected header items in activities CSV file (expecting 68 items): {list(keys)}"
            )
        keys.extend(["count", "csv_file_name"])
        
        id_field = keys[0]
        date_field = keys[1]
        type_field = keys[3]
        gear_field = keys[9]
        filename_field = keys[10]
        return [
            {
                "id": a[id_field],
                "type": a[type_field],
                "date": a[date_field],
                "filename": a[filename_field],
                "activity_count": len(activities),
                "csv_temp_file": csv_file_name,
            }
            for a in activities
        ]


def main():
    start_time = datetime.now()
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "--input",
        "-i",
        dest="strava_export",
        metavar="ZIPFILE_OR_DIR",
        type=str,
        required=True,
        help="A Strava export zip file, or a directory containing the unzipped Strava export to work on.",
    )
    args_parser.add_argument(
        "--output",
        "-o",
        dest="output_dir",
        metavar="DIR",
        type=str,
        help="Put generated GPX files into this directory.",
    )
    args_parser.add_argument(
        "--filter-type",
        "-f",
        dest="filter_types",
        metavar="ACTIVITY_TYPE",
        type=str,
        action="append",
        help="Only convert activities with the given ACTIVITY_TYPE. May be used multiple times. Use --list-types to find out what types exist.",
    )
    args_parser.add_argument(
        "--list-types",
        "-l",
        dest="list_types",
        action="store_true",
        help="List all activity types found in the Strava export directory.",
    )
    args_parser.add_argument(
        "--filter-year",
        "-y",
        dest="filter_years",
        metavar="YEAR",
        type=str,
        action="append",
        help="Only convert activities with the given YEAR. May be used multiple times.",
    )

    args_parser.add_argument(
        "--verbose", "-v", dest="verbose", action="store_true", help="Verbose output."
    )

    args = args_parser.parse_args()

    if os.path.isdir(args.strava_export):
        zip_file = None
        activities_csv = os.path.join(args.strava_export, "activities.csv")
    else:
        zip_file = zipfile.ZipFile(args.strava_export, "r")
        activities_csv = "activities.csv"

    if args.list_types:
        if args.output_dir or args.filter_types:
            print_usage_error(
                args_parser,
                "error: you cannot use --output or --filter-type together with --list-types\n",
            )
        print(f"Activity types found in {args.strava_export}:")
        for activity_type in sorted(
            list(
                set(
                    [
                        activity["type"]
                        for activity in get_activities(zip_file, activities_csv)
                    ]
                )
            )
        ):
            print(f"- {activity_type}")
    else:
        if not args.output_dir:
            print_usage_error(
                args_parser,
                "error: either --output or --list-types must be specified\n",
            )
        os.makedirs(args.output_dir, exist_ok=True)

        for activity in get_activities(zip_file, activities_csv):
            activity_file_name = activity["filename"]
            activity_date = date_format(activity["date"]).strftime("%Y-%m-%dT%H%M%S")

            if not activity_file_name:
                continue

            if not zip_file:
                activity_file_name = os.path.join(
                    args.strava_export, activity_file_name
                )

            if not matches_filter_years(activity_date, args.filter_years):
                if args.verbose:
                    print(
                        f'Skipping {activity_file_name}, year={activity_date["date"][0:4]}.'
                    )
                continue

            if not matches_filter_types(activity, args.filter_types):
                if args.verbose:
                    print(f'Skipping {activity_file_name}, type={activity["type"]}.')
                continue

            gpx_file_name = f"{activity_date}_{activity['type']}_{activity['id']}.gpx"
            gpx_file_path = os.path.join(args.output_dir, gpx_file_name)

            if args.verbose:
                print(f"Converting {activity_file_name} to {gpx_file_path}.")
            if zip_file:
                with tempfile.NamedTemporaryFile(
                    suffix=os.path.basename(activity_file_name), delete=False
                ) as unzipped_file:
                    zip_extract(zip_file, activity_file_name, unzipped_file)
                    convert_activity(unzipped_file.name, gpx_file_path)
            else:
                convert_activity(activity_file_name, gpx_file_path)
                
        stray_file = activity["csv_temp_file"]
        ac_count = activity["activity_count"]
        clear_temp(args.verbose, stray_file)

    end_time = datetime.now()
    print(f"All done! {ac_count} converted activities are in: {args.output_dir} - Total time: {str(end_time - start_time).split('.')[0]}")


if __name__ == "__main__":
    main()
