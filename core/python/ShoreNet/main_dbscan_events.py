# -*- encoding: utf-8 -*-
'''
@File    :   main_dbscan_events.py
@Time    :   2025/02/16 18:28:02
@Author  :   Jiayu(Jerry) Qiu
@Version :   1.0
@Contact :   qiujiayu0212@gmail.com
@Desc    :   dbscan events which are not able to match with dock polygons.
'''


import argparse
import os
import traceback
import numpy as np
import pandas as pd

from scipy.spatial import ConvexHull, QhullError
from simplification.cutil import simplify_coords

from core.ShoreNet.definitions.variables import ShoreNetVariablesManager
from core.ShoreNet.definitions.parameters import ArgsDefinition as Ad, ColumnNames as Cn
from core.ShoreNet.events.generic.tools import load_events_without_dock
from core.ShoreNet.events.polygon import cluster_dock_polygon_dbscan


def simplify_polygon(polygon, tolerance):
    # return simplify_coords(polygon, tolerance)
    return polygon


# Function to compute convex hull and return the coordinates
def compute_convex_hull(cluster_id, cluster_points, max_points=8):
    if len(cluster_points) > 2:  # Convex hull requires at least 3 points
        try:
            hull = ConvexHull(cluster_points)
            hull_points = cluster_points[hull.vertices]
            
            if len(hull_points) > max_points:
                simplified_hull_points = simplify_polygon(hull_points, tolerance=0.002)
                if len(simplified_hull_points) <= 3:
                    output_points = hull_points
                else:
                    output_points = simplified_hull_points
            else:
                output_points = hull_points

            output_points = np.append(output_points, output_points[0])  # Append the first point to close the hull
            points_array = np.array([[x, y] for x, y in zip(output_points[::2], output_points[1::2])])
            return points_array
        except QhullError as e:
            print(f"QhullError for cluster {cluster_id}: {e}")
    else:
        return cluster_points  # For less than 3 points, just use the points themselves


def generate_points_hulls(df):
    plot_row_list = []
    hull_df_list = []
    for cluster_id, group in df.groupby('cluster'):
        # NOTE: please increase the number of unique mmsi and event count for real data
        #       recommend：(group['mmsi'].nunique() < 20) & (group.shape[0] < 90)
        #       if want to loosen or tighten the condition, please decrease or increase the number
        if (group['mmsi'].nunique() < 1) & (group.shape[0] < 1):
            continue
        
        _row = group.iloc[0, :]
        _d = {
            'cluster': cluster_id, 
            'lng': _row[Cn.lng], 
            'lat': _row[Cn.lat], 
            'size': group.shape[0]
        }
        plot_row_list.append(_d)

        # get hull data
        num_rows = group.shape[0]
        num_select = int(num_rows * 1.0)

        # Randomly select row indices
        selected_indices = np.random.choice(num_rows, num_select, replace=False)
        hull_df_list.append(group.iloc[selected_indices, :])

    plot_df = pd.DataFrame(plot_row_list)
    hull_df = pd.concat(hull_df_list, ignore_index=True)
    hull_df.loc[:, 'mmsi'] = hull_df['mmsi'].astype(str)
    return plot_df, hull_df


def hull_polygon_by_cluster(hull_df, cluster_name):
    unique_clusters = hull_df[cluster_name].unique()
    chlls_dict = {}
    for cluster in unique_clusters:
        if cluster == -1:  # Skip noise points
            continue
        
        cluster_points = hull_df[hull_df[cluster_name] == cluster][[Cn.lng, Cn.lat]].values

        if len(cluster_points) > 2:  # Convex hull requires at least 3 points
            try:
                hull_points = compute_convex_hull(cluster_id=cluster, cluster_points=cluster_points)
                chlls_dict[cluster] = hull_points
            except:
                traceback.print_exc()
                print(f"cluster_id : {cluster} hull failed.")
    return chlls_dict


def plot_on_map(plot_df, hull_df, cluster_name):
    import plotly.express as px
    import plotly.graph_objects as go
    from scipy.spatial import ConvexHull

    fig = go.Figure()

    # Create a scatter mapbox plot with clusters
    fig = px.scatter_mapbox(
        plot_df,
        lat="lat",
        lon="lng",
        color=cluster_name,
        size="size",
        hover_name=cluster_name,
        hover_data={"size": True, "lat": True, "lng": True},
        title="Clusters on Map",
        color_continuous_scale=px.colors.cyclical.IceFire,
        zoom=5,
        height=600
    )
    
    # Calculate and add the convex hull for each cluster
    unique_clusters = hull_df[cluster_name].unique()
    
    for cluster in unique_clusters:
        if cluster == -1:  # Skip noise points
            continue
        cluster_points = hull_df[hull_df[cluster_name] == cluster][[Cn.lng, Cn.lat]].values
    
        if len(cluster_points) > 2:  # Convex hull requires at least 3 points
            try:
                hull = ConvexHull(cluster_points)
                hull_points = np.append(hull.vertices, hull.vertices[0])  # Append the first point to close the hull
                fig.add_trace(go.Scattermapbox(
                    lon=cluster_points[hull_points, 0],
                    lat=cluster_points[hull_points, 1],
                    mode='lines',
                    fill='toself',
                    fillcolor='rgba(0, 0, 255, 0.1)',  # Semi-transparent fill color
                    line=dict(color='blue'),
                    name=f'Cluster {cluster} Hull'
                ))
            except:
                print(f"cluster_id : {cluster} hull failed.")
    
    # Set the map style and layout
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_center={"lat": plot_df["lat"].mean(), "lon": plot_df["lng"].mean()},
        margin={"r":0,"t":0,"l":0,"b":0},
        height=700, width=1200
    )
    
    fig.write_html("private/cluster_map.html")


def run_app():
    parser = argparse.ArgumentParser(description='process match polygon for events')
    parser.add_argument(f"--{Ad.stage_env}", type=str, required=True, help='Process stage name')
    parser.add_argument(f'--{Ad.year}', type=int, required=True, help='Process year')
    args = parser.parse_args()
    stage_env = args.__getattribute__(Ad.stage_env)
    year = args.__getattribute__(Ad.year)
    
    vars = ShoreNetVariablesManager(stage_env)
    
    # -. load events without coal_dock_id
    dbscan_events_df = cluster_dock_polygon_dbscan(
        events_df=load_events_without_dock(year=year, vars=vars),
        vars=vars
    )
    
    # plot them on map
    plot_df, hull_df = generate_points_hulls(dbscan_events_df)
    cluster_hulls_dict = hull_polygon_by_cluster(hull_df, 'cluster')
    plot_on_map(plot_df, hull_df, 'cluster')
    print("done")


if __name__ == "__main__":
    run_app()