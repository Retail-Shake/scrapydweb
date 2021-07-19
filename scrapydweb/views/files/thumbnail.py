# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from ...utils.monitoring_tools import dataframes as rsd
from ..baseview import BaseView


class ThumbnailView(BaseView):
    def __init__(self, *args, **kwargs):
        super(ThumbnailView, self).__init__(*args, **kwargs)

        self.spider = self.view_args["spider"]

    def dispatch_request(self, **kwargs):
        df = rsd.sqlite_to_df(
            where=f"spider = '{self.spider}' AND julianday('now') - julianday(start) <= 14"
        )
        return df.to_json(orient="records")
