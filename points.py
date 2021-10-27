#!/usr/bin/env python3

from contextlib import closing
import sqlite3

points = {}

with closing(sqlite3.connect("ddnet-server.sqlite")) as con:
    con.row_factory = sqlite3.Row
    with closing(con.cursor()) as cur:
        cur.execute("DELETE FROM record_points")
        cur.execute("SELECT DISTINCT Name, Map FROM record_race")
        finishes = cur.fetchall()

        for finish in finishes:
            if not finish["Map"] in points:
                cur.execute("SELECT Points FROM record_maps WHERE Map = ?", [finish["Map"]])
                points_row = cur.fetchone()
                if points_row is None:
                    print("Failed to look up points for map '%s'" % finish["Map"])
                    exit(1)
                points[finish["Map"]] = points_row["Points"]
            cur.execute(
                "INSERT INTO record_points (Name, Points) "
                "VALUES (?, ?) "
                "ON CONFLICT(Name) DO UPDATE SET Points=Points+?;",
                (finish["Name"], points[finish["Map"]], points[finish["Map"]])
                )
            print("%s +%d %s" % (finish["Map"], points[finish["Map"]], finish["Name"]))
    con.commit()
