#!/usr/bin/env python3

import argparse
import csv
import gzip
import os
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime
from typing import IO, Any, Dict, List, Optional, Tuple

import gpxpy.gpx  # type: ignore
import lxml.etree  # type: ignore
from fitparse import FitFile  # type: ignore
from tcxparser import TCXParser  # type: ignore

# TCX namespaces, maps
TCD_NAMESPACE = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
TCD = "{%s}" % TCD_NAMESPACE
XML_SCHEMA_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance"
XML_SCHEMA = "{%s}" % XML_SCHEMA_NAMESPACE
SCHEMA_LOCATION = (
    "http://www.garmin.com/xmlschemas/ActivityExtension/v2 "
    + "http://www.garmin.com/xmlschemas/ActivityExtensionv2.xsd "
    + "http://www.garmin.com/xmlschemas/FatCalories/v1 "
    + "http://www.garmin.com/xmlschemas/fatcalorieextensionv1.xsd "
    + "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2 "
    + "http://www.garmin.com/xmlschemas/TrainingCenterDatabasev2.xsd"
)
NSMAP = {None: TCD_NAMESPACE, "xsi": XML_SCHEMA_NAMESPACE}
SPORT_MAP = {"running": "Running", "cycling": "Biking"}


def semicircle_to_degrees(semicircles: int) -> float:
    """ WGS-84 semicircle 32-bit to degree conversion """
    return semicircles * (180.0 / 2.0 ** 31)


def format_float_str(number: float) -> str:
    """ Format float as rstripped string for tcx files """
    return "{:.10f}".format(number).rstrip("0").rstrip(".")


def create_tcx_element(
    tag: str, text: Optional[str] = None, namespace: Optional[str] = None
) -> lxml.etree._Element:
    """ Create single namespaced tcx element """
    namespace = NSMAP[namespace]
    tag = "{%s}%s" % (namespace, tag)
    element = lxml.etree.Element(tag, nsmap=NSMAP)
    if text is not None:
        element.text = text
    return element


def create_tcx_subelement(
    parent: lxml.etree._Element,
    tag: str,
    text: Optional[str] = None,
    namespace: Optional[str] = None,
) -> lxml.etree._Element:
    """ Create tcx subelement """
    element = create_tcx_element(tag, text, namespace)
    parent.append(element)
    return element


def create_tcx_document() -> lxml.etree._Element:
    """ Add schema and element to tcx document """
    tcx_document = create_tcx_element("TrainingCenterDatabase")
    tcx_document.set(XML_SCHEMA + "schemaLocation", SCHEMA_LOCATION)
    tcx_document = lxml.etree.ElementTree(tcx_document)
    return tcx_document


def add_tcx_trackpoint(element: lxml.etree._Element, trackpoint: Any) -> Any:
    """ Check properties and form tcx trackpoint """
    timestamp = trackpoint.get_value("timestamp")
    pos_lat = trackpoint.get_value("position_lat")
    pos_long = trackpoint.get_value("position_long")
    distance = trackpoint.get_value("distance")
    altitude = trackpoint.get_value("altitude")
    heart_rate = trackpoint.get_value("heart_rate")

    create_tcx_subelement(element, "Time", timestamp.isoformat() + "Z")

    if pos_lat is not None and pos_long is not None:
        pos = create_tcx_subelement(element, "Position")
        create_tcx_subelement(
            pos, "LatitudeDegrees", format_float_str(semicircle_to_degrees(pos_lat))
        )
        create_tcx_subelement(
            pos, "LongitudeDegrees", format_float_str(semicircle_to_degrees(pos_long))
        )

    if altitude is not None:
        create_tcx_subelement(element, "AltitudeMeters", format_float_str(altitude))

    if distance is not None:
        create_tcx_subelement(element, "DistanceMeters", format_float_str(distance))

    if heart_rate is not None:
        heartrateelem = create_tcx_subelement(element, "HeartRateBpm")
        heartrateelem.set(XML_SCHEMA + "type", "HeartRateInBeatsPerMinute_t")
        create_tcx_subelement(heartrateelem, "Value", format_float_str(heart_rate))


def add_tcx_lap(element: lxml.etree._Element, activity: Any, lap: Any) -> Any:
    """ Lap parsing and creation for tcx """
    start_time = lap.get_value("start_time")
    end_time = lap.get_value("timestamp")

    totaltime = lap.get_value("total_elapsed_time")
    if totaltime is None:
        totaltime = lap.get_value("")

    lapelem = create_tcx_subelement(element, "Lap")
    lapelem.set("StartTime", start_time.isoformat() + "Z")

    trackelem = create_tcx_subelement(lapelem, "Track")
    for trackpoint in activity.get_messages(name="record"):
        tts = trackpoint.get_value("timestamp")
        if start_time <= tts <= end_time:
            trackpointelem = create_tcx_subelement(trackelem, "Trackpoint")
            add_tcx_trackpoint(trackpointelem, trackpoint)


def add_tcx_activity(element: lxml.etree._Element, activity: Any) -> Any:
    """ Create tcx activity """
    session = next(activity.get_messages(name="session"))

    sport = SPORT_MAP.get(session.get_value("sport"), "Other")
    identity = session.get_value("start_time")

    activity_element = create_tcx_subelement(element, "Activity")
    activity_element.set("Sport", sport)
    create_tcx_subelement(activity_element, "Id", identity.isoformat() + "Z")

    for lap in activity.get_messages("lap"):
        add_tcx_lap(activity_element, activity, lap)


def date_format(text: str) -> datetime:
    """ Convert date notations for gpx """
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%b %d, %Y, %I:%M:%S %p",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError("No valid date format found")


def convert_fit_tcx(filename: str) -> lxml.etree._Element:
    """ Convert fit file to tcx using the fitparse module """
    tcx_document = create_tcx_document()
    element = create_tcx_subelement(tcx_document.getroot(), "Activities")

    activity = FitFile(filename)
    activity.parse()
    add_tcx_activity(element, activity)

    return tcx_document


def convert_tcx_gpx(tcx_path: str) -> lxml.etree._Element:
    """ Convert tcx file to gpx using the tcxparser module """
    gpx = gpxpy.gpx.GPX()
    tcx = TCXParser(str(tcx_path))
    track_points = zip(tcx.position_values(), tcx.altitude_points(), tcx.time_values())
    gpx.name = date_format(tcx.started_at).strftime("%Y-%m-%d %H:%M:%S")
    gpx.description = ""
    gpx_track = gpxpy.gpx.GPXTrack(
        name=date_format(tcx.started_at).strftime("%Y-%m-%d %H:%M:%S"), description="",
    )
    gpx_track.type = tcx.activity_type
    gpx.tracks.append(gpx_track)
    gpx_segment = gpxpy.gpx.GPXTrackSegment()
    gpx_track.segments.append(gpx_segment)
    for track_point in track_points:
        gpx_trackpoint = gpxpy.gpx.GPXTrackPoint(
            latitude=track_point[0][0],
            longitude=track_point[0][1],
            elevation=track_point[1],
            time=date_format(track_point[2]),
        )
        gpx_segment.points.append(gpx_trackpoint)
    gpx_document = gpx.to_xml()
    return gpx_document


def matches_filter_types(activity: Dict, filter_types: Optional[List]) -> bool:
    """ Filters activities by type """
    if not filter_types:
        return True
    activity_type = activity["type"].lower()
    for filter_type in filter_types:
        if filter_type.lower() == activity_type:
            return True
    return False


def matches_filter_years(activity_date: str, filter_years: Optional[List]) -> bool:
    """ Filters activities by year """
    if not filter_years:
        return True
    activity_year = activity_date[0:4]
    if activity_year in filter_years:
        return True
    return False


def matches_filter_gear(activity: Dict, filter_gear: Optional[List]) -> bool:
    """ Filters activity by gear """
    if not filter_gear:
        return True
    activity_gear = activity["gear"].lower()
    for gear_filter in filter_gear:
        if gear_filter.lower() == activity_gear:
            return True
    return False


def write_gpx_file(source: str, dest: str) -> None:
    """ Write the gpx file to disk """
    gpx_document = convert_tcx_gpx(source)
    dest = str(dest.replace(".tcx", ".gpx"))
    with open(dest, "w") as output:
        output.write(gpx_document)


def gps_track_convert(
    input_file_path: str, output_file_path: str, file_type: str
) -> None:
    """ Convert fit and tcx to gpx using lxml """
    if file_type == "fit":
        tcx_document = convert_fit_tcx(input_file_path)
        output_file_path = str(output_file_path.replace(".gpx", ".tcx"))
        with open(output_file_path, "w") as output:
            tcx_document = lxml.etree.tostring(
                tcx_document.getroot(),
                pretty_print=True,
                xml_declaration=True,
                encoding="UTF-8",
            )
            output.write(tcx_document.decode("utf-8"))
        write_gpx_file(output_file_path, output_file_path)
        os.unlink(output_file_path)
    if file_type == "tcx":
        write_gpx_file(input_file_path, output_file_path)


def zip_extract(zip_file: zipfile.ZipFile, file_name: str, target_file_obj: IO) -> None:
    """ Unzip and copy to temp dir """
    with zip_file.open(file_name) as file_target:
        shutil.copyfileobj(file_target, target_file_obj)
        target_file_obj.flush()


def convert_activity(activity_file_name: str, target_gpx_file_name: str) -> None:
    """ Unzip packed files and convert """
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
        gps_track_convert(activity_file_name, target_gpx_file_name, "fit")

    elif activity_file_name.endswith(".tcx"):
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            with open(activity_file_name, "r") as file_to_strip:
                for line in file_to_strip:
                    stripped_line = line.strip() + "\n"
                    temp.write(stripped_line.encode("utf-8"))
            temp.close()
            gps_track_convert(temp.name, target_gpx_file_name, "tcx")
            os.unlink(temp.name)

    elif activity_file_name.endswith(".gpx"):
        shutil.copyfile(activity_file_name, target_gpx_file_name)

    else:
        print(f"Unrecognized/unsupported file format: {activity_file_name}\n")


def print_usage_error(args_parser: argparse.ArgumentParser, message: str) -> None:
    """ Print usage error """
    args_parser.print_usage()
    sys.stderr.write(message)
    sys.exit(2)


def get_activities_csv(
    zip_file: Optional[zipfile.ZipFile], csv_file_name: str
) -> List[Dict]:
    """ Handle the activities.csv content and return dict """
    if zip_file:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as unzipped_file:
            zip_extract(zip_file, csv_file_name, unzipped_file)
            return get_activities_csv(None, unzipped_file.name)
    with open(csv_file_name, encoding="utf8") as csv_file:
        activities = list(csv.DictReader(csv_file))
        activity_count = len(activities)
        if activity_count == 0:
            return []
        keys = list(activities[0].keys())
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
                "gear": a[gear_field],
                "filename": a[filename_field],
                "activity_count": activity_count,
                "csv_temp_file": csv_file_name,
            }
            for a in activities
        ]


def check_zip(
    zip_file: Optional[zipfile.ZipFile], gpx_file_path: str, activity_file_name: str
) -> None:
    """ Check for zip file """
    if zip_file:
        with tempfile.NamedTemporaryFile(
            suffix=os.path.basename(activity_file_name), delete=False
        ) as unzipped_file:
            zip_extract(zip_file, activity_file_name, unzipped_file)
            convert_activity(unzipped_file.name, gpx_file_path)
        os.unlink(unzipped_file.name)
    else:
        convert_activity(activity_file_name, gpx_file_path)


def extract_zip(args: str) -> Tuple[Optional[zipfile.ZipFile], str]:
    """ Unpack main archive and get activities.csv """
    if os.path.isdir(args):
        zip_file = None
        activities_csv = os.path.join(args, "activities.csv")
    else:
        zip_file = zipfile.ZipFile(args, "r")
        activities_csv = "activities.csv"
    return zip_file, activities_csv


def clear_temp(args: str, stray_file: str) -> None:
    """ Checks for temp file and deletes it if necessary """
    if os.path.exists(stray_file):
        try:
            if args:
                print(f"Removing tempfile: {stray_file}")
            os.unlink(stray_file)
        except Exception as exception_msg:
            print(f"Error while deleting file {exception_msg}")


def main() -> None:
    """ Convert all activities and write to disk """
    start_time = datetime.now()
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument(
        "-i",
        "--input",
        dest="strava_export",
        metavar="ZIPFILE_OR_DIR",
        type=str,
        required=True,
        help="A Strava export zip file, or a directory containing the unzipped Strava export to work on",
    )
    args_parser.add_argument(
        "-o",
        "--output",
        dest="output_dir",
        metavar="DIR",
        type=str,
        help="Put generated GPX files into this directory",
    )
    args_parser.add_argument(
        "-ft",
        "--filter-type",
        dest="filter_types",
        metavar="ACTIVITY_TYPE",
        type=str,
        action="append",
        help="Only convert activities with the given ACTIVITY_TYPE. May be used multiple times. Use --list-types to find out what types exist",
    )
    args_parser.add_argument(
        "-y",
        "--filter-year",
        dest="filter_years",
        metavar="YEAR",
        type=str,
        action="append",
        help="Only convert activities with the given YEAR. May be used multiple times",
    )
    args_parser.add_argument(
        "-g",
        "--filter-gear",
        dest="filter_gear",
        metavar="ACTIVITY_GEAR",
        type=str,
        action="append",
        help="Only convert activities with the given ACTIVITY_GEAR. May be used multiple times. Use --list-gear to find out what gear exists",
    )
    args_parser.add_argument(
        "-lt",
        "--list-types",
        dest="list_types",
        action="store_true",
        help="List all activity types found in the Strava export directory",
    )
    args_parser.add_argument(
        "-lg",
        "--list-gear",
        dest="list_gear",
        action="store_true",
        help="List all gear found in the Strava export directory",
    )

    args_parser.add_argument(
        "-v", "--verbose", dest="verbose", action="store_true", help="Verbose output"
    )
    activity = None
    args = args_parser.parse_args(args=None if sys.argv[1:] else ["--help"])
    zip_file, activity_csv = extract_zip(args.strava_export)
    if args.list_types:
        if args.output_dir or args.filter_types:
            print_usage_error(
                args_parser,
                "Error: You cannot use --output or --filter-type together with --list-types or --list-gear\n",
            )
        print(f"Activity types found in {args.strava_export}:")
        for activity_type in sorted(
            list(
                set(
                    [
                        activity["type"]
                        for activity in get_activities_csv(zip_file, activity_csv)
                    ]
                )
            )
        ):
            print(f"- {activity_type}")
            ac_count, filter_control_var = None, None

    elif args.list_gear:
        if args.output_dir or args.filter_types:
            print_usage_error(
                args_parser,
                "Error: You cannot use --output or --filter-type together with --list-types or --list-gear\n",
            )
        print(f"Gear found in {args.strava_export}:")
        for gear_list in sorted(
            list(
                set(
                    [
                        activity["gear"]
                        for activity in get_activities_csv(zip_file, activity_csv)
                    ]
                )
            )
        ):
            print(f"- {gear_list}")
            ac_count, filter_control_var = None, None

    else:
        if not args.output_dir:
            print_usage_error(
                args_parser,
                "Error: Either --output or --list-types must be specified\n",
            )
        os.makedirs(args.output_dir, exist_ok=True)
        activity_control_var, filter_control_var = 0, 0
        for activity in get_activities_csv(zip_file, activity_csv):
            activity_control_var += 1
            sys.stdout.write(
                f" Converting activities: {str(activity_control_var)} / {activity['activity_count']}\r"
            )
            activity_file_name = activity["filename"]
            activity_date = date_format(activity["date"]).strftime("%Y-%m-%dT%H%M%S")
            if not activity_file_name:
                continue

            if not zip_file:
                activity_file_name = os.path.join(
                    args.strava_export, activity_file_name
                )

            if not matches_filter_years(activity_date, args.filter_years):
                filter_control_var += 1
                if args.verbose:
                    print(f"Skipping {activity_file_name}, year={activity_date[0:4]}")
                continue

            if not matches_filter_types(activity, args.filter_types):
                filter_control_var += 1
                if args.verbose:
                    print(f'Skipping {activity_file_name}, type={activity["type"]}')
                continue

            if not matches_filter_gear(activity, args.filter_gear):
                filter_control_var += 1
                if args.verbose:
                    print(f'Skipping {activity_file_name}, gear={activity["gear"]}')
                continue

            gpx_file_name = f"{activity_date}_{activity['type']}_{activity['id']}.gpx"
            gpx_file_path = os.path.join(args.output_dir, gpx_file_name)

            if args.verbose:
                print(f"Converting {activity_file_name} to {gpx_file_path}")

            check_zip(zip_file, gpx_file_path, activity_file_name)

        stray_file = activity["csv_temp_file"]
        ac_count = activity["activity_count"]
        clear_temp(args.verbose, stray_file)

    end_time = datetime.now()
    print(
        f"Successfully processed {ac_count} activities ({filter_control_var} filtered) - Total time: [{str(end_time - start_time).split('.')[0]}]"
    )


if __name__ == "__main__":
    main()
