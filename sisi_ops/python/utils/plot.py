"""
@Author  ： Jerry Qiu
@Email   :  qiujiayu0212@gmail.com
@FileName:  plot.py
@DateTime:  23/11/2024 9:03 am
@DESC    :  python plot utils
"""

import plotly.express as px
import plotly.io as pio


def plot_scatter(df, x_col, y_col, output_file_path):
    # Plot the clusters
    fig = px.scatter_mapbox(
        df,
        lat=y_col,
        lon=x_col,
        color='cluster_id',
        title='DBSCAN Clusters',
        color_continuous_scale=px.colors.cyclical.IceFire,
        zoom=15,
        height=600
    )

    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_center={
            "lat": df[y_col].mean(),
            "lon": df[x_col].mean()
        },
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=700, width=1200
    )

    pio.write_html(fig, file=output_file_path, auto_open=False)
