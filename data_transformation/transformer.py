import json
import logging
import os
from glob import glob

import pandas as pd
from tqdm import tqdm

# Set up logger for the module.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class DataTransformer:
    def __init__(self, json_folder=None, output_folder=None):
        """
        Initializes the transformer with folders for JSON input and processed outputs.
        The folders are set relative to the project root.
        """
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        logger.info(f"Project root determined as: {project_root}")

        if not json_folder:
            json_folder = os.path.join(project_root, "data", "extracted")
        if not output_folder:
            output_folder = os.path.join(project_root, "data", "processed")

        self.json_folder = json_folder
        self.output_folder = output_folder

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            logger.info(f"Created output folder: {self.output_folder}")

        self.tests = []
        self.odis = []
        self.t20s = []
        self.deliveries = []

    def load_json_files(self):
        """
        Loads all JSON files from the json_folder recursively.
        """
        file_pattern = os.path.join(self.json_folder, "**", "*.json")
        file_list = glob(file_pattern, recursive=True)
        logger.info(f"Found {len(file_list)} JSON files in {self.json_folder}.")
        return file_list

    def process_files(self):
        """
        Processes each JSON file, extracting match metadata and deliveries.
        """
        files = self.load_json_files()
        for file_path in tqdm(files, desc="Processing JSON files"):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    match_json = json.load(f)
                self._process_match(match_json, file_path)
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")

    def _process_match(self, match_json, file_path):
        """
        Processes a single match JSON object.
        Extracts detailed match metadata and aggregates delivery records.
        """
        info = match_json.get("info", {})

        match_id = os.path.splitext(os.path.basename(file_path))[0]
        match_type = info.get("match_type", "").lower()

        outcome_by_dict = info.get("outcome", {}).get("by", {})
        if outcome_by_dict:
            outcome_type = list(outcome_by_dict.keys())[0]
            outcome_by = outcome_by_dict[outcome_type]
        else:
            outcome_type = None
            outcome_by = None

        match_data = {
            "match_id": match_id,
            "match_type": match_type,
            "season": info.get("season"),
            "venue": info.get("venue"),
            "city": info.get("city"),
            "dates": info.get("dates", []),
            "teams": info.get("teams", []),
            "toss_winner": info.get("toss", {}).get("winner"),
            "toss_decision": info.get("toss", {}).get("decision"),
            "outcome_result": info.get("outcome", {}).get("result"),
            "outcome_winner": info.get("outcome", {}).get("winner"),
            "outcome_type": outcome_type,
            "outcome_by": outcome_by,
            "event_name": info.get("event", {}).get("name"),
            "event_match_number": info.get("event", {}).get("match_number"),
            "balls_per_over": info.get("balls_per_over"),
            "match_type_number": info.get("match_type_number"),
            "overs": info.get("overs"),
            "gender": info.get("gender"),
            "officials": info.get("officials"),
            "player_of_match": info.get("player_of_match"),
            "team_type": info.get("team_type"),
        }

        if "test" in match_type:
            self.tests.append(match_data)
        elif "odi" in match_type:
            self.odis.append(match_data)
        elif "t20" in match_type:
            self.t20s.append(match_data)
        else:
            logger.info(f"Uncategorized match type in file {file_path}: {match_type}")

        innings = match_json.get("innings", [])
        for inning_index, inning in enumerate(innings, start=1):
            batting_team = inning.get("team")
            for over in inning.get("overs", []):
                over_num = over.get("over")
                for delivery_index, delivery in enumerate(over.get("deliveries", []), start=1):
                    delivery_record = {
                        "match_id": match_id,
                        "innings": inning_index,
                        "batting_team": batting_team,
                        "over": over_num,
                        "delivery_in_over": delivery_index,
                        "batter": delivery.get("batter"),
                        "bowler": delivery.get("bowler"),
                        "non_striker": delivery.get("non_striker"),
                    }
                    runs = delivery.get("runs", {})
                    delivery_record["runs_batter"] = runs.get("batter")
                    delivery_record["runs_extras"] = runs.get("extras")
                    delivery_record["runs_total"] = runs.get("total")

                    wickets = delivery.get("wickets", [])
                    if wickets:
                        first_wicket = wickets[-1]
                        delivery_record["wicket_kind"] = first_wicket.get("kind")
                        delivery_record["wicket_player_out"] = first_wicket.get("player_out")
                        if "fielders" in first_wicket and first_wicket["fielders"]:
                            delivery_record["wicket_fielders"] = [f.get("name") for f in first_wicket["fielders"] if
                                                                  f.get("name")]
                        else:
                            delivery_record["wicket_fielders"] = []

                    self.deliveries.append(delivery_record)

    def fill_event_match_number(self, df):
        """
        For each group (by season and event_name), sorts by match_id and fills missing event_match_number values
        in sequence order. If some rows already have a number, new values start after the max existing number.
        """
        def fill_group(group):
            group = group.sort_values("match_id")
            group["event_match_number"] = group["event_match_number"].replace("", pd.NA)
            group["event_match_number"] = pd.to_numeric(group["event_match_number"], errors="coerce")
            missing_mask = group["event_match_number"].isna()
            if missing_mask.any():
                if group["event_match_number"].notna().any():
                    start = int(group["event_match_number"].max()) + 1
                else:
                    start = 1
                group.loc[missing_mask, "event_match_number"] = range(start, start + missing_mask.sum())
            return group

        return df.groupby(["season", "event_name"], group_keys=False).apply(fill_group)

    def impute_match_fields(self, df, combined):
        """
        For each row in the match DataFrame (df), imputes missing fields using the combined DataFrame (from all match types).
        - If 'city' is empty, attempts to fill it from a record with the same venue.
        - If outcome_type is not None and outcome_result is missing, sets outcome_result to "win".
        - If event_name is empty, attempts to fill it using records with the same teams (and city, if possible).
        """
        for idx, row in df.iterrows():
            if pd.isna(row["city"]) or row["city"] == "":
                subset = combined[
                    (combined["venue"] == row["venue"]) &
                    (combined["city"].notna()) & (combined["city"] != "")
                    ]
                if not subset.empty:
                    df.at[idx, "city"] = subset.iloc[0]["city"]
            if pd.notna(row["outcome_type"]) and (pd.isna(row["outcome_result"]) or row["outcome_result"] in ["", None]):
                df.at[idx, "outcome_result"] = "win"
            if pd.isna(row["event_name"]) or row["event_name"] == "":
                try:
                    teams_key = tuple(sorted(row["teams"]))
                except Exception:
                    teams_key = row["teams"]
                subset = combined[
                    (combined["event_name"].notna()) & (combined["event_name"] != "") &
                    (combined["teams"].apply(lambda t: tuple(sorted(t)) if isinstance(t, list) else t) == teams_key) &
                    ((combined["city"] == row["city"]) |
                     ((pd.isna(row["city"]) or row["city"] == "") & (combined["city"] != "")))
                    ]
                if not subset.empty:
                    df.at[idx, "event_name"] = subset.iloc[0]["event_name"]
                else:
                    subset = combined[
                        (combined["event_name"].notna()) & (combined["event_name"] != "") &
                        (combined["teams"].apply(lambda t: tuple(sorted(t)) if isinstance(t, list) else t) == teams_key)
                        ]
                    if not subset.empty:
                        df.at[idx, "event_name"] = subset.iloc[0]["event_name"]
        return df

    def get_dataframes(self):
        """
        Converts the extracted match and delivery data into Pandas DataFrames.
        Produces four DataFrames:
          1. Test matches (match metadata)
          2. ODI matches (match metadata)
          3. T20 matches (match metadata)
          4. Deliveries (raw delivery records)
        Then applies imputation on the match DataFrames and fills missing event_match_number.
        """
        df_tests = pd.DataFrame(self.tests)
        df_odis = pd.DataFrame(self.odis)
        df_t20s = pd.DataFrame(self.t20s)
        df_deliveries = pd.DataFrame(self.deliveries)

        logger.info(
            f"Created DataFrames - Tests: {df_tests.shape}, ODIs: {df_odis.shape}, "
            f"T20s: {df_t20s.shape}, Deliveries: {df_deliveries.shape}"
        )

        combined = pd.concat([df_tests, df_odis, df_t20s], ignore_index=True)

        df_tests = self.impute_match_fields(df_tests, combined)
        df_odis = self.impute_match_fields(df_odis, combined)
        df_t20s = self.impute_match_fields(df_t20s, combined)

        if "event_match_number" not in df_tests.columns:
            df_tests["event_match_number"] = pd.NA
        if "event_match_number" not in df_odis.columns:
            df_odis["event_match_number"] = pd.NA
        if "event_match_number" not in df_t20s.columns:
            df_t20s["event_match_number"] = pd.NA

        df_tests = self.fill_event_match_number(df_tests)
        df_odis = self.fill_event_match_number(df_odis)
        df_t20s = self.fill_event_match_number(df_t20s)

        return df_tests, df_odis, df_t20s, df_deliveries

    def save_dataframes(self):
        """
        Saves the DataFrames as CSV files in the output folder.
        Four CSV files are saved:
          - test_matches.csv
          - odi_matches.csv
          - t20_matches.csv
          - deliveries.csv
        """
        df_tests, df_odis, df_t20s, df_deliveries = self.get_dataframes()

        tests_path = os.path.join(self.output_folder, "test_matches.csv")
        odis_path = os.path.join(self.output_folder, "odi_matches.csv")
        t20s_path = os.path.join(self.output_folder, "t20_matches.csv")
        deliveries_path = os.path.join(self.output_folder, "deliveries.csv")

        df_tests.to_csv(tests_path, index=False)
        df_odis.to_csv(odis_path, index=False)
        df_t20s.to_csv(t20s_path, index=False)
        df_deliveries.to_csv(deliveries_path, index=False)

        logger.info("DataFrames saved as CSV files:")
        logger.info(f"Test Matches: {tests_path}")
        logger.info(f"ODI Matches: {odis_path}")
        logger.info(f"T20 Matches: {t20s_path}")
        logger.info(f"Deliveries: {deliveries_path}")


if __name__ == "__main__":
    transformer = DataTransformer()
    transformer.process_files()
    transformer.save_dataframes()
