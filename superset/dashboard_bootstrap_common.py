import json
import uuid


def metric_card(datasource, name, metric, subheader, time_range, y_axis_format="SMART_NUMBER"):
    return {
        "datasource": datasource,
        "slice_name": name,
        "viz_type": "big_number_total",
        "params": {
            "viz_type": "big_number_total",
            "metric": metric,
            "subheader": subheader,
            "y_axis_format": y_axis_format,
            "time_range": time_range,
            "rolling_type": "None",
            "show_metric_name": False,
            "header_font_size": 0.28,
            "subheader_font_size": 0.12,
            "y_axis_bounds": [None, None],
        },
    }


def bar_chart(datasource, name, metric, groupby, time_range, row_limit=20):
    return {
        "datasource": datasource,
        "slice_name": name,
        "viz_type": "dist_bar",
        "params": {
            "viz_type": "dist_bar",
            "metrics": [metric],
            "groupby": groupby,
            "columns": [],
            "time_range": time_range,
            "adhoc_filters": [],
            "row_limit": row_limit,
            "order_desc": True,
            "show_legend": False,
            "y_axis_format": "SMART_NUMBER",
            "color_scheme": "supersetColors",
        },
    }


def pie_chart(datasource, name, metric, groupby, time_range, row_limit=10, label_colors=None):
    spec = {
        "datasource": datasource,
        "slice_name": name,
        "viz_type": "pie",
        "params": {
            "viz_type": "pie",
            "metric": metric,
            "groupby": [groupby],
            "time_range": time_range,
            "adhoc_filters": [],
            "row_limit": row_limit,
            "sort_by_metric": True,
            "show_legend": True,
            "legendOrientation": "right",
            "show_labels": True,
            "label_type": "key_percent",
            "number_format": "SMART_NUMBER",
            "donut": False,
            "color_scheme": "supersetColors",
        },
    }
    if label_colors:
        spec["params"]["label_colors"] = json.dumps(label_colors, ensure_ascii=False)
    return spec


def timeseries(datasource, name, metrics, x_axis, time_range, groupby=None, y_axis_format="SMART_NUMBER"):
    return {
        "datasource": datasource,
        "slice_name": name,
        "viz_type": "echarts_timeseries_line",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "x_axis": x_axis,
            "time_grain_sqla": "P1D",
            "time_range": time_range,
            "metrics": metrics,
            "groupby": groupby or [],
            "adhoc_filters": [],
            "row_limit": 10000,
            "show_legend": True,
            "legendOrientation": "top",
            "rich_tooltip": True,
            "markerEnabled": True,
            "markerSize": 4,
            "y_axis_format": y_axis_format,
            "color_scheme": "supersetColors",
        },
    }


def table_chart(datasource, name, groupby, metrics, time_range, row_limit=500):
    return {
        "datasource": datasource,
        "slice_name": name,
        "viz_type": "table",
        "params": {
            "viz_type": "table",
            "groupby": groupby,
            "metrics": metrics,
            "time_range": time_range,
            "adhoc_filters": [],
            "row_limit": row_limit,
            "include_search": True,
            "page_length": 25,
            "order_desc": True,
        },
    }


def create_chart(db, Slice, spec, description):
    datasource = spec["datasource"]
    chart = (
        db.session.query(Slice)
        .filter_by(slice_name=spec["slice_name"], datasource_id=datasource.id)
        .one_or_none()
    )
    if chart is None:
        chart = Slice(
            slice_name=spec["slice_name"],
            datasource_type="table",
            datasource_id=datasource.id,
            datasource_name=f"{datasource.schema}.{datasource.table_name}",
            viz_type=spec["viz_type"],
            created_by_fk=1,
            changed_by_fk=1,
            uuid=uuid.uuid4(),
        )
        db.session.add(chart)
        db.session.flush()

    params = dict(spec["params"])
    params["slice_id"] = chart.id
    params["datasource"] = f"{datasource.id}__table"
    params["dashboards"] = []
    chart.viz_type = spec["viz_type"]
    chart.params = json.dumps(params, ensure_ascii=False)

    metrics = params.get("metrics") or ([params["metric"]] if params.get("metric") else [])
    columns = list(params.get("groupby", []))
    x_axis = params.get("x_axis")
    if x_axis and x_axis not in columns:
        columns.insert(0, x_axis)
    query_columns = []
    for column in columns:
        if column == x_axis and params.get("time_grain_sqla"):
            query_columns.append(
                {
                    "timeGrain": params["time_grain_sqla"],
                    "columnType": "BASE_AXIS",
                    "sqlExpression": column,
                    "label": column,
                    "expressionType": "SQL",
                }
            )
        else:
            query_columns.append(column)

    query = {
        "filters": [],
        "extras": {"having": "", "where": ""},
        "applied_time_extras": {},
        "columns": query_columns,
        "metrics": metrics,
        "orderby": [[metrics[0], False]] if metrics else [],
        "annotation_layers": [],
        "row_limit": params.get("row_limit", 10000),
        "series_columns": [],
        "series_limit": 0,
        "order_desc": params.get("order_desc", True),
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
        "time_offsets": [],
        "time_range": params.get("time_range", "No filter"),
    }
    chart.query_context = json.dumps(
        {
            "datasource": {"id": datasource.id, "type": "table"},
            "force": False,
            "queries": [query],
            "form_data": params,
            "result_format": "json",
            "result_type": "full",
        },
        ensure_ascii=False,
    )
    chart.description = description
    return chart


def add_row(layout, root, grid, row_id, charts, height):
    layout[row_id] = {
        "id": row_id,
        "type": "ROW",
        "parents": [root, grid],
        "children": [],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    width = 12 // len(charts)
    for chart in charts:
        node = f"CHART-{chart.id}"
        layout[node] = {
            "id": node,
            "type": "CHART",
            "parents": [root, grid, row_id],
            "children": [],
            "meta": {
                "chartId": chart.id,
                "sliceName": chart.slice_name,
                "uuid": str(chart.uuid),
                "width": width,
                "height": height,
            },
        }
        layout[row_id]["children"].append(node)


def time_filter(filter_id, name, dataset_ids, default_time_range, root):
    return {
        "id": filter_id,
        "name": name,
        "filterType": "filter_time",
        "targets": [{"datasetId": dataset_id} for dataset_id in dataset_ids],
        "defaultDataMask": {
            "extraFormData": {"time_range": default_time_range},
            "filterState": {"value": default_time_range},
        },
        "cascadeParentIds": [],
        "scope": {"rootPath": [root], "excluded": []},
        "type": "NATIVE_FILTER",
        "description": "",
    }


def select_filter(filter_id, name, targets, root):
    return {
        "id": filter_id,
        "controlValues": {
            "enableEmptyFilter": False,
            "defaultToFirstItem": False,
            "multiSelect": True,
            "searchAllOptions": True,
            "inverseSelection": False,
        },
        "name": name,
        "filterType": "filter_select",
        "targets": [
            {"column": {"name": column}, "datasetId": dataset_id}
            for dataset_id, column in targets
        ],
        "defaultDataMask": {"extraFormData": {}, "filterState": {}},
        "cascadeParentIds": [],
        "scope": {"rootPath": [root], "excluded": []},
        "type": "NATIVE_FILTER",
        "description": "",
    }


def dashboard_metadata(charts, root, native_filters, refresh_frequency=0):
    chart_ids = [chart.id for chart in charts]
    return {
        "chart_configuration": {
            str(chart.id): {
                "id": chart.id,
                "crossFilters": {"scope": "global", "chartsInScope": []},
            }
            for chart in charts
        },
        "global_chart_configuration": {
            "scope": {"rootPath": [root], "excluded": []},
            "chartsInScope": chart_ids,
        },
        "color_scheme": "supersetColors",
        "shared_label_colors": {},
        "color_scheme_domain": [],
        "expanded_slices": {},
        "label_colors": {},
        "timed_refresh_immune_slices": [],
        "default_filters": "{}",
        "refresh_frequency": refresh_frequency,
        "cross_filters_enabled": True,
        "native_filter_configuration": native_filters,
    }


def remove_legacy_dashboard(db, Dashboard, title):
    legacy = db.session.query(Dashboard).filter_by(dashboard_title=title).one_or_none()
    if legacy is not None:
        legacy.slices = []
        db.session.delete(legacy)
        db.session.flush()
