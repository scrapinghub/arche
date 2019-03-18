import plotly.graph_objs as go


def scraped_fields_coverage(job, df):
    coverage_values = df.count().sort_values(ascending=True).values / len(df) * 100
    fields = df.count().sort_values(ascending=True).index

    trace = go.Bar(
        x=coverage_values,
        y=fields,
        name=job,
        opacity=0.75,
        orientation="h",
        marker=dict(
            color="rgba(50, 171, 96, 0.2)",
            line=dict(color="rgba(50, 171, 96, 1.0)", width=1),
        ),
    )
    layout = go.Layout(
        title="Scraped Fields Coverage",
        height=100 + len(df.columns) * 20,
        margin=dict(t=40, b=70, l=200, r=20),
        xaxis=dict(showgrid=False, showline=False, showticklabels=True, zeroline=False),
        yaxis=dict(showgrid=False, showline=False, showticklabels=True, zeroline=False),
    )

    annotations = []

    for yd, xd in zip(fields, coverage_values):
        annotations.append(
            dict(
                xref="x",
                yref="y",
                x=xd + 5,
                y=yd,
                text=str(round(xd, 1)) + "%",
                font=dict(family="Arial", size=12, color="rgb(50, 171, 96)"),
                showarrow=False,
            )
        )

    layout["annotations"] = annotations

    fig = go.Figure(data=[trace], layout=layout)
    return fig


def scraped_items_history(job_no, job_numbers, date_items):
    prod_x_data = [date_items[i].keys()[0] for i in range(len(date_items))]
    prod_y_data = [date_items[i].values()[0] for i in range(len(date_items))]
    bar_colors = []
    for job in job_numbers:
        if job != job_no:
            bar_colors.append("rgb(204,204,204)")
        else:
            bar_colors.append("rgb(112,194,99)")

    trace = go.Bar(
        x=prod_x_data,
        y=prod_y_data,
        text=job_numbers,
        marker=dict(color=bar_colors),
        name="prod",
    )

    layout = go.Layout(
        title="<b>Scraped Items History</b>",
        margin=dict(t=25, b=25, l=25, r=25),
        xaxis=dict(
            title="Run Date",
            showgrid=False,
            showline=False,
            showticklabels=True,
            zeroline=False,
            domain=[0.1, 1],
        ),
        yaxis=dict(
            title="Number of Scraped Items",
            showgrid=False,
            showline=False,
            showticklabels=True,
            zeroline=False,
            domain=[0.1, 1],
        ),
    )

    fig = go.Figure(data=[trace], layout=layout)
    return fig
