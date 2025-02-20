# coding: utf-8
import json
import pandas as pd

from flask import render_template, url_for, current_app

from ..baseview import BaseView
from ...utils.monitoring_tools import dataframes as mtd, maths as mtm, graphs as mtg
from ...vars import DATABASE_URL


class NodeReportsRSView(BaseView):
    def __init__(self):
        super(NodeReportsRSView, self).__init__()

        self.url = url_for("jobs", node=self.node, listjobs="True")
        self.text = ""
        self.jobs = []
        self.pending_jobs = []
        self.running_jobs = []
        self.finished_jobs = []
        self.template = "scrapydweb/node_reports_rs.html"

    def dispatch_request(self, **kwargs):
        self.text = self.get_response_from_view(self.url, as_json=False)
        try:
            self.jobs = json.loads(self.text)
        except ValueError as err:
            self.logger.error("Fail to decode json from %s: %s", self.url, err)
            return self.text

        for job in self.jobs:
            if not job["start"]:
                self.pending_jobs.append(job)
            else:
                if job["finish"]:
                    try:
                        spider_filter = f"spider = '{job['spider']}'"
                        table_name = self.SCRAPYD_SERVER.replace(".", "_").replace(
                            ":", "_"
                        )
                        con, db_type = mtd.db_connect(DATABASE_URL, return_db_type=True)
                        if db_type == "sqlite":
                            query = f"""
                                SELECT * 
                                FROM '{table_name}'
                                WHERE {spider_filter}
                                """
                            df = mtd.jobs_df_format(pd.read_sql(query, con=con))
                        elif db_type == "mysql":
                            query = f"""
                            SELECT *
                            FROM {table_name}
                            WHERE {spider_filter};
                            """
                            df = mtd.jobs_df_format(pd.read_sql(query, con=con))
                        else:
                            self.logger("Database type not handled yet...")
                            return

                        df = mtd.jobs_df_format(pd.read_sql(query, con=con))

                        #
                        df = mtd.select_last_date(df, "start_date")

                        # Compute means
                        df = mtm.compute_floating_means(
                            df, "items", 7
                        )  # Compute floating mean for items
                        df = mtm.compute_floating_means(
                            df, "pages", 7
                        )  # Compute floating mean for pages

                        # Compute standard deviations
                        df = mtm.compute_floating_deviation(df, "items", 7)
                        df = mtm.compute_floating_deviation(df, "pages", 7)

                        # Set alert lvl
                        items_alert_lvl = mtm.set_alert_level(
                            df, "items"
                        )  # issue #1279 → variable 'scrap_result' referenced before assignment
                        job["alert_indicator"] = mtm.check_alert_level(items_alert_lvl)

                        self.finished_jobs.append(job)

                    except Exception as e:
                        self.logger.error(f"Failed to get data from database:\n{e}")
                else:
                    job["alert_indicator"] = "🔄"
                    self.running_jobs.append(job)

        if self.JOBS_FINISHED_JOBS_LIMIT > 0:
            self.finished_jobs = self.finished_jobs[::-1][
                : self.JOBS_FINISHED_JOBS_LIMIT
            ]
        else:
            self.finished_jobs = self.finished_jobs[::-1]
        kwargs = dict(
            node=self.node,
            url=self.url,
            pending_jobs=self.pending_jobs,
            running_jobs=self.running_jobs,
            finished_jobs=self.finished_jobs,
            url_report=url_for(
                "log",
                node=self.node,
                opt="report",
                project="PROJECT_PLACEHOLDER",
                spider="SPIDER_PLACEHOLDER",
                job="JOB_PLACEHOLDER",
            ),
            url_schedule=url_for("schedule", node=self.node),
        )
        return render_template(self.template, **kwargs)
