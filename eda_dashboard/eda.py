import logging
import os

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.io as pio
import seaborn as sns
import toml
from sqlalchemy import create_engine
from tqdm import tqdm

# Set up aesthetics.
sns.set(style="whitegrid")
pio.templates.default = "plotly_white"

# Set up logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

class EDAAnalyzer:
    def __init__(self, connection_string, viz_folder):
        """
        Initializes the analyzer with a database connection (SQLAlchemy engine using pymysql)
        and a folder for saving visualizations.
        """
        self.engine = create_engine(connection_string, echo=False)
        self.viz_folder = viz_folder
        if not os.path.exists(self.viz_folder):
            os.makedirs(self.viz_folder)
            logger.info(f"Created visualizations folder: {self.viz_folder}")

    def run_query(self, query):
        """Executes a SQL query and returns a DataFrame."""
        try:
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as e:
            logger.error(f"Error running query: {e}")
            return pd.DataFrame()

    # 1. Top 10 Batsmen in ODI by Total Runs
    def viz_top10_odi_batsmen(self):
        query = """
        SELECT d.batter AS `player`, SUM(d.runs_batter) AS `total_runs`
        FROM deliveries d
                 INNER JOIN odi_matches m ON d.match_id = m.match_id
        GROUP BY d.batter
        ORDER BY total_runs DESC
        LIMIT 10;
        """
        df = self.run_query(query)
        if 'player' not in df.columns or df.empty:
            logger.error("Query1: 'player' column missing or empty.")
            return
        plt.figure(figsize=(10,6))
        sns.barplot(x="player", y="total_runs", data=df, palette="Blues_d", dodge=False)
        plt.xlabel("Batsman")
        plt.ylabel("Total Runs")
        plt.title("Top 10 Batsmen in ODI by Total Runs")
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query1_top10_odi_batsmen.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query1 visualization to {path}")

    # 2. Top 10 Bowlers in T20 by Wickets
    def viz_top10_t20_bowlers(self):
        query = """
            SELECT `d`.`bowler` AS `player`, COUNT(*) AS `wickets`
            FROM `deliveries` `d`
                     INNER JOIN `t20_matches` `m` ON `d`.`match_id` = `m`.`match_id`
            WHERE `d`.`wicket_kind` IS NOT NULL
            GROUP BY `d`.`bowler`
            ORDER BY `wickets` DESC
            LIMIT 10;
        """
        df = self.run_query(query)
        if 'player' not in df.columns or df.empty:
            logger.error("Query2: 'player' column missing or empty.")
            return
        plt.figure(figsize=(10,6))
        sns.barplot(x="player", y="wickets", data=df, palette="Reds_d", dodge=False)
        plt.xlabel("Bowler")
        plt.ylabel("Wickets")
        plt.title("Top 10 Bowlers in T20 by Wickets")
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query2_top10_t20_bowlers.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query2 visualization to {path}")

    # 3. Team Win Percentage in Test Matches
    def viz_test_team_win_percentage(self):
        query = """
        SELECT teams, 
               COUNT(*) AS total_matches,
               SUM(CASE WHEN outcome_result = 'win' THEN 1 ELSE 0 END) AS wins,
               ROUND(100 * SUM(CASE WHEN outcome_result = 'win' THEN 1 ELSE 0 END) / COUNT(*),2) AS win_percentage
        FROM test_matches
        GROUP BY teams
        ORDER BY win_percentage DESC;
        """
        df = self.run_query(query)
        if 'teams' not in df.columns or df.empty:
            logger.error("Query3: 'teams' column missing or empty.")
            return
        plt.figure(figsize=(10,6))
        sns.barplot(x="teams", y="win_percentage", data=df, hue="teams", dodge=False, legend=False)
        plt.xlabel("Team(s)")
        plt.ylabel("Win Percentage")
        plt.title("Team Win Percentage in Test Matches")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query3_test_team_win_percentage.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query3 visualization to {path}")

    # 4. Match Outcome Distribution Across All Formats (Pie)
    def viz_outcome_distribution(self):
        query = """
        SELECT outcome_result, COUNT(*) AS count FROM (
          SELECT outcome_result FROM test_matches
          UNION ALL
          SELECT outcome_result FROM odi_matches
          UNION ALL
          SELECT outcome_result FROM t20_matches
        ) AS all_matches
        GROUP BY outcome_result;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query4 returned an empty dataframe.")
            return
        fig = px.pie(df, values="count", names="outcome_result", title="Match Outcome Distribution (All Formats)")
        path = os.path.join(self.viz_folder, "query4_outcome_distribution.html")
        fig.write_html(path)
        logger.info(f"Saved Query4 visualization to {path}")

    # 5. Average Margin of Victory in ODI Matches by Season
    def viz_odi_avg_margin(self):
        query = """
        SELECT season, AVG(CAST(outcome_by AS DECIMAL(10,2))) AS avg_margin
        FROM odi_matches
        WHERE outcome_type = 'runs'
        GROUP BY season
        ORDER BY season;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query5 returned an empty dataframe.")
            return
        plt.figure(figsize=(10,6))
        sns.lineplot(x="season", y="avg_margin", data=df, marker="o")
        plt.xlabel("Season")
        plt.ylabel("Average Margin (Runs)")
        plt.title("Average Margin of Victory in ODI Matches by Season")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query5_odi_avg_margin.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query5 visualization to {path}")

    # 6. Top 5 ODI Batsmen Trend by Season
    def viz_odi_top5_batsmen_trend(self):
        query6a = """
        SELECT d.batter AS player, SUM(d.runs_batter) AS total_runs
        FROM deliveries d
        INNER JOIN odi_matches m ON d.match_id = m.match_id
        GROUP BY d.batter
        ORDER BY total_runs DESC
        LIMIT 5;
        """
        df6a = self.run_query(query6a)
        if 'player' not in df6a.columns or df6a.empty:
            logger.error("Query6a: 'player' column missing or dataframe is empty.")
            return
        top5 = df6a['player'].tolist()
        query6 = f"""
        SELECT m.season, d.batter AS player, SUM(d.runs_batter) AS total_runs
        FROM deliveries d
        INNER JOIN odi_matches m ON d.match_id = m.match_id
        WHERE d.batter IN ({','.join(["'"+p+"'" for p in top5])})
        GROUP BY m.season, d.batter
        ORDER BY m.season;
        """
        df6 = self.run_query(query6)
        if df6.empty:
            logger.error("Query6 returned an empty dataframe.")
            return
        plt.figure(figsize=(10,6))
        sns.lineplot(x="season", y="total_runs", hue="player", data=df6, marker="o")
        plt.xlabel("Season")
        plt.ylabel("Total Runs")
        plt.title("Total Runs Trend by Season for Top 5 ODI Batsmen")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query6_odi_top5_batsmen_trend.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query6 visualization to {path}")

    # 7. Test Matches Won per Season
    def viz_test_wins_by_season(self):
        query = """
        SELECT season, COUNT(*) AS wins
        FROM test_matches
        WHERE outcome_result = 'win'
        GROUP BY season
        ORDER BY season;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query7 returned an empty dataframe.")
            return
        plt.figure(figsize=(10,6))
        sns.lineplot(x="season", y="wins", data=df, marker="o")
        plt.xlabel("Season")
        plt.ylabel("Matches Won")
        plt.title("Test Matches Won per Season")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query7_test_wins_by_season.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query7 visualization to {path}")

    # 8. Toss Decisions vs Outcomes in T20 (Stacked Bar)
    def viz_t20_toss_vs_outcome(self):
        query = """
        SELECT toss_decision, outcome_result, COUNT(*) AS count
        FROM t20_matches
        GROUP BY toss_decision, outcome_result;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query8 returned an empty dataframe.")
            return
        df_pivot = df.pivot(index="toss_decision", columns="outcome_result", values="count").fillna(0)
        df_pivot.plot(kind="bar", stacked=True, figsize=(10,6), colormap="Accent")
        plt.xlabel("Toss Decision")
        plt.ylabel("Count")
        plt.title("Toss Decisions vs Match Outcomes in T20 Matches")
        plt.xticks(rotation=0)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query8_t20_toss_vs_outcome.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query8 visualization to {path}")

    # 9. Margin of Victory Distribution in ODI Matches
    def viz_odi_margin_distribution(self):
        query = """
        SELECT CAST(outcome_by AS DECIMAL(10,2)) AS margin
        FROM odi_matches
        WHERE outcome_type = 'runs';
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query9 returned an empty dataframe.")
            return
        plt.figure(figsize=(10,6))
        sns.histplot(df['margin'].dropna(), bins=20, kde=True, color="purple")
        plt.xlabel("Margin (Runs)")
        plt.ylabel("Frequency")
        plt.title("Distribution of Margin of Victory in ODI Matches")
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query9_odi_margin_distribution.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query9 visualization to {path}")

    # 10. Top 10 Venues in All Matches
    def viz_top10_venues(self):
        query = """
        SELECT venue, COUNT(*) AS count FROM (
          SELECT venue FROM test_matches
          UNION ALL
          SELECT venue FROM odi_matches
          UNION ALL
          SELECT venue FROM t20_matches
        ) AS all_matches
        GROUP BY venue
        ORDER BY count DESC
        LIMIT 10;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query10 returned an empty dataframe.")
            return
        plt.figure(figsize=(12,6))
        sns.barplot(x="venue", y="count", data=df, hue="venue", dodge=False, legend=False)
        plt.xlabel("Venue")
        plt.ylabel("Count")
        plt.title("Top 10 Venues in All Matches")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query10_top10_venues.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query10 visualization to {path}")

    # 11. Top 5 Match Winners in T20 Matches
    def viz_top5_t20_winners(self):
        query = """
        SELECT outcome_winner AS team, COUNT(*) AS wins
        FROM t20_matches
        WHERE outcome_result = 'win'
        GROUP BY outcome_winner
        ORDER BY wins DESC
        LIMIT 5;
        """
        df = self.run_query(query)
        if 'team' not in df.columns or df.empty:
            logger.error("Query11: 'team' column missing or dataframe is empty.")
            return
        plt.figure(figsize=(10,6))
        sns.barplot(x="team", y="wins", data=df, hue="team", dodge=False, legend=False)
        plt.xlabel("Team")
        plt.ylabel("Wins")
        plt.title("Top 5 Match Winners in T20 Matches")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query11_top5_t20_winners.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query11 visualization to {path}")

    # 12. Player of the Match Frequency in ODI Matches
    def viz_odi_pom_frequency(self):
        query = """
        SELECT REPLACE(TRIM(SUBSTRING_INDEX(SUBSTRING(`player_of_match`, 2, LENGTH(`player_of_match`) - 2), ',', 1)), "'",
               '') AS `player`,
        COUNT(*)    AS `frequency`
        FROM `odi_matches`
        WHERE `player_of_match` IS NOT NULL
        GROUP BY `player`
        ORDER BY `frequency` DESC
        LIMIT 10;
        """
        df = self.run_query(query)
        if 'player' not in df.columns or df.empty:
            logger.error("Query12: 'player' column missing or dataframe is empty.")
            return
        plt.figure(figsize=(10,6))
        sns.barplot(x="player", y="frequency", data=df, hue="player", dodge=False, legend=False)
        plt.xlabel("Player")
        plt.ylabel("Frequency")
        plt.title("Player of the Match Frequency in ODI Matches")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query12_odi_pom_frequency.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query12 visualization to {path}")

    # 13. Distribution of Overs in Test Matches
    def viz_test_overs_distribution(self):
        query = """
        SELECT `overs`
        FROM `test_matches`
        WHERE `overs` IS NOT NULL ;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query13 returned an empty dataframe.")
            return
        plt.figure(figsize=(10,6))
        sns.histplot(df['overs'].dropna(), bins=15, kde=True, color="orange")
        plt.xlabel("Overs")
        plt.ylabel("Frequency")
        plt.title("Distribution of Overs in Test Matches")
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query13_test_overs_distribution.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query13 visualization to {path}")

    # 14. Scatter Plot: Match Type Number vs Overs in T20 Matches
    def viz_t20_scatter(self):
        query = """
        SELECT match_type_number, overs
        FROM t20_matches;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query14 returned an empty dataframe.")
            return
        plt.figure(figsize=(10,6))
        sns.scatterplot(x="match_type_number", y="overs", data=df, hue="match_type_number", legend=False)
        plt.xlabel("Match Type Number")
        plt.ylabel("Overs")
        plt.title("T20 Matches: Match Type Number vs Overs")
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query14_t20_scatter.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query14 visualization to {path}")

    # 15. Total Deliveries per Season in ODI Matches
    def viz_odi_deliveries_trend(self):
        query = """
        SELECT m.season, COUNT(*) AS total_deliveries
        FROM deliveries d
        INNER JOIN odi_matches m ON d.match_id = m.match_id
        GROUP BY m.season
        ORDER BY m.season;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query15 returned an empty dataframe.")
            return
        plt.figure(figsize=(10,6))
        sns.lineplot(x="season", y="total_deliveries", data=df, marker="o")
        plt.xlabel("Season")
        plt.ylabel("Total Deliveries")
        plt.title("Total Deliveries per Season in ODI Matches")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query15_odi_deliveries_trend.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query15 visualization to {path}")

    # 16. Top 5 Bowlers with Best Economy in Test Matches
    def viz_test_best_economy(self):
        query = """
        SELECT d.bowler AS player, SUM(d.runs_total)/COUNT(*) AS economy, COUNT(*) AS deliveries
        FROM deliveries d
        INNER JOIN test_matches m ON d.match_id = m.match_id
        GROUP BY d.bowler
        HAVING deliveries > 50
        ORDER BY economy ASC
        LIMIT 5;
        """
        df = self.run_query(query)
        if df.empty or 'player' not in df.columns:
            logger.error("Query16 returned an empty dataframe or missing 'player' column.")
            return
        plt.figure(figsize=(10,6))
        sns.barplot(x="player", y="economy", data=df, hue="player", dodge=False, legend=False)
        plt.xlabel("Bowler")
        plt.ylabel("Economy")
        plt.title("Top 5 Bowlers with Best Economy in Test Matches")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query16_test_best_economy.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query16 visualization to {path}")

    # 17. Frequency of Toss Winners in ODI Matches
    def viz_odi_toss_winner(self):
        query = """
        SELECT toss_winner AS team, COUNT(*) AS frequency
        FROM odi_matches
        GROUP BY toss_winner
        ORDER BY frequency DESC;
        """
        df = self.run_query(query)
        if df.empty or 'team' not in df.columns:
            logger.error("Query17 returned an empty dataframe or missing 'team' column.")
            return
        plt.figure(figsize=(10,6))
        sns.barplot(x="team", y="frequency", data=df, hue="team", dodge=False, legend=False)
        plt.xlabel("Team")
        plt.ylabel("Frequency")
        plt.title("Frequency of Toss Winners in ODI Matches")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query17_odi_toss_winner.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query17 visualization to {path}")

    # 18. T20 Match Outcome Trends by Season (Stacked Bar)
    def viz_t20_outcome_trends(self):
        query = """
        SELECT season, outcome_result, COUNT(*) AS count
        FROM t20_matches
        GROUP BY season, outcome_result
        ORDER BY season;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query18 returned an empty dataframe.")
            return
        df_pivot = df.pivot(index="season", columns="outcome_result", values="count").fillna(0)
        df_pivot.plot(kind="bar", stacked=True, figsize=(12,7), colormap="Paired")
        plt.xlabel("Season")
        plt.ylabel("Number of Matches")
        plt.title("T20 Match Outcome Trends by Season")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query18_t20_outcome_trends.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query18 visualization to {path}")

    # 19. Top 10 Cities by Number of Matches (All Formats)
    def viz_top10_cities(self):
        query = """
        SELECT city, COUNT(*) AS count FROM (
          SELECT city FROM test_matches
          UNION ALL
          SELECT city FROM odi_matches
          UNION ALL
          SELECT city FROM t20_matches
        ) AS all_matches
        GROUP BY city
        ORDER BY count DESC
        LIMIT 10;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query19 returned an empty dataframe.")
            return
        plt.figure(figsize=(12,6))
        sns.barplot(x="city", y="count", data=df, hue="city", dodge=False, legend=False)
        plt.xlabel("City")
        plt.ylabel("Number of Matches")
        plt.title("Top 10 Cities by Number of Matches (All Formats)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query19_top10_cities.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query19 visualization to {path}")

    # 20. Correlation Heatmap of Numeric Attributes in ODI Matches
    def viz_odi_correlation_heatmap(self):
        query = """
        SELECT match_type_number, overs, CAST(outcome_by AS DECIMAL(10,2)) AS outcome_by_numeric
        FROM odi_matches
        WHERE outcome_by IS NOT NULL;
        """
        df = self.run_query(query)
        if df.empty:
            logger.error("Query20 returned an empty dataframe.")
            return
        plt.figure(figsize=(8,6))
        sns.heatmap(df.corr(), annot=True, cmap="coolwarm")
        plt.title("Correlation Heatmap in ODI Matches")
        plt.tight_layout()
        path = os.path.join(self.viz_folder, "query20_odi_correlation_heatmap.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved Query20 visualization to {path}")

    def run_all(self):
        """Runs all 20 visualization methods."""
        methods = [
            self.viz_top10_odi_batsmen,
            self.viz_top10_t20_bowlers,
            self.viz_test_team_win_percentage,
            self.viz_outcome_distribution,
            self.viz_odi_avg_margin,
            self.viz_odi_top5_batsmen_trend,
            self.viz_test_wins_by_season,
            self.viz_t20_toss_vs_outcome,
            self.viz_odi_margin_distribution,
            self.viz_top10_venues,
            self.viz_top5_t20_winners,
            self.viz_odi_pom_frequency,
            self.viz_test_overs_distribution,
            self.viz_t20_scatter,
            self.viz_odi_deliveries_trend,
            self.viz_test_best_economy,
            self.viz_odi_toss_winner,
            self.viz_t20_outcome_trends,
            self.viz_top10_cities,
            self.viz_odi_correlation_heatmap,
        ]
        for method in tqdm(methods, desc="Generating visualizations"):
            method()

if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    config_path = os.path.join(project_root, "config.toml")
    config = toml.load(config_path)
    connection_string = config["db"]["connection_string"]
    viz_folder = os.path.join(project_root, "data", "visualizations")
    analyzer = EDAAnalyzer(connection_string, viz_folder)
    analyzer.run_all()
