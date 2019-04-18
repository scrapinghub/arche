import plotly.graph_objs as go


def scraped_fields_coverage(job, df) -> go.FigureWidget:
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
    return go.FigureWidget(data=[trace], layout=layout)
