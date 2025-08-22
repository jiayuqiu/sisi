import pandas as pd
import numpy as np
import pymssql
import plotly.express as px
import plotly.graph_objects as go
from scipy.spatial import ConvexHull, QhullError
from simplification.cutil import simplify_coords

import time
import platform
import os
import sys
import traceback

parent_path = os.path.abspath('.')
sys.path.append(parent_path)
print(parent_path)
parent_path = os.path.abspath('..')
sys.path.append(parent_path)
print(parent_path)
parent_path = os.path.abspath('../../')
sys.path.append(parent_path)
print(parent_path)
parent_path = os.path.abspath('../../../')
print(parent_path)
sys.path.append(parent_path)
print(sys.path)

from sklearn.cluster import DBSCAN

from sisi_ops.ShoreNet.conf import sql_server_properties


os_name = platform.system()
if os.name == 'nt' or os_name == 'Windows':
    DATA_PATH = r"D:/data/sisi/"
elif os.name == 'posix' or os_name == 'Linux':
    DATA_PATH = r"/mnt/d/data/sisi/"
else:
    DATA_PATH = r"/mnt/d/data/sisi/"

STAGE_ID = 4


def load_event_without_polygon(start_month, end_month):
    months = [f"2023{x:02}" for x in range(start_month, end_month+1)]

    without_polygon_df_list = []
    for month in months:
        _df = pd.read_csv(f"{DATA_PATH}/extensive_coal_events/stage_{STAGE_ID}/{month}.csv")
        without_polygon_df_list.append(_df)
    without_polygon_df = pd.concat(without_polygon_df_list, ignore_index=True)
    return without_polygon_df


def dbscan_events(df):
    coords = df[['lng', 'lat']].values

    kms_per_radian = 6371.0088
    epsilon = 0.2 / kms_per_radian

    # DBSCAN clustering
    db = DBSCAN(eps=epsilon, min_samples=30, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
    return db.labels_


def simplify_polygon(polygon, tolerance):
    return simplify_coords(polygon, tolerance)


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
        if (group['mmsi'].nunique() < 20) & (group.shape[0] < 90):
            continue
        
        _row = group.iloc[0, :]
        _d = {'cluster': cluster_id, 'lng': _row['lng'], 'lat': _row['lat'], 'size': group.shape[0]}
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
        
        cluster_points = hull_df[hull_df[cluster_name] == cluster][['lng', 'lat']].values

        if len(cluster_points) > 2:  # Convex hull requires at least 3 points
            try:
                hull_points = compute_convex_hull(cluster_id=cluster, cluster_points=cluster_points)
                chlls_dict[cluster] = hull_points
            except:
                traceback.print_exc()
                print(f"cluster_id : {cluster} hull failed.")
    return chlls_dict


def plot_on_map(plot_df, hull_df, cluster_name):
    fig = go.Figure()
    
    color_scale = [
        [0, 'green'],
        [1, 'red']
    ]

    # Create a scatter mapbox plot with clusters
    fig = px.scatter_mapbox(
        plot_df,
        lat="lat",
        lon="lng",
        # color="size",
        # size="size",
        hover_name=cluster_name,
        hover_data={"size": True, "lat": True, "lng": True},
        title="Clusters on Map",
        color_continuous_scale=color_scale,
        zoom=5,
        height=600
    )
    
    fig.update_traces(marker=dict(size=15, opacity=0.2))

    # Calculate and add the convex hull for each cluster
    unique_clusters = hull_df[cluster_name].unique()

    for cluster in unique_clusters:
        if cluster == -1:  # Skip noise points
            continue
        
        cluster_points = hull_df[hull_df[cluster_name] == cluster][['lng', 'lat']].values

        if len(cluster_points) > 3:  # Convex hull requires at least 4 points
            try:
                hull_points = compute_convex_hull(cluster_id=cluster, cluster_points=cluster_points)
                fig.add_trace(go.Scattermapbox(
                    lon=hull_points[:, 0],
                    lat=hull_points[:, 1],
                    mode='lines',
                    fill='toself',
                    fillcolor='rgba(0, 0, 255, 0.1)',  # Semi-transparent fill color
                    line=dict(color='blue'),
                    name=f'Cluster {cluster} Hull'
                ))
            except:
                traceback.print_exc()
                print(f"cluster_id : {cluster} hull failed.")

    # Set the map style and layout
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_center={"lat": plot_df["lat"].mean(), "lon": plot_df["lng"].mean()},
        margin={"r":0,"t":0,"l":0,"b":0},
        height=700, width=1200
    )

    fig.write_html('/mnt/c/Users/qiu/IdeaProjects/SISI/core/ShoreNet/notebooks/html/new_stage_4.html')
    # fig.show()
    print('plot done.!')


def hull_points_to_sql(c_points_dict):
    conn = pymssql.connect(sql_server_properties['host'], sql_server_properties['user'], 
                           'Amacs@0212', sql_server_properties['database'])
    cursor = conn.cursor()

    # Insert polygon data
    insert_query = """
    INSERT INTO ShoreNet.tab_dock_polygon (Name, type_id, lng, lat, Polygon, stage_id)
    VALUES ('%s', %d, %f, %f, geometry::STGeomFromText('%s', 4326), %d);
    """
    
    utc_time = int(time.time())
    for cluster, points in c_points_dict.items():
        cluster_polygon_points = []
        
        for point in points:
            cluster_polygon_points.append([round(point[0], 6), round(point[1], 6)])
        # cluster_polygon_points.append([round(points[0][0], 6), round(points[0][1], 6)])  # append start point to the end
        
        if len(cluster_polygon_points) < 4:
            print(f"cluster: {cluster} have {len(cluster_polygon_points)} points. Can not insert.")
            continue
        name = f"dbscan_cluster_{utc_time}_{cluster}"
        polygon_wkt = f"POLYGON(({', '.join([f'{lon} {lat}' for lon, lat in cluster_polygon_points])}))"
        type_id = 6
        lng = cluster_polygon_points[0][0]
        lat = cluster_polygon_points[0][1]
        q = insert_query % (name, type_id, lng, lat, polygon_wkt, STAGE_ID)
        cursor.execute(q)
    
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()
    print('hull points saved.')
    


def main():
    without_polygon_df = load_event_without_polygon(1, 12)
    # without_polygon_df = without_polygon_df.loc[without_polygon_df['lat'] < 29.75]
    without_polygon_df.loc[:, 'cluster'] = dbscan_events(without_polygon_df)
    without_polygon_df = without_polygon_df.loc[without_polygon_df['cluster']!=-1]
    plot_df, hull_df = generate_points_hulls(without_polygon_df)
    # plot_on_map(plot_df, hull_df, 'cluster')
    cluster_hulls_dict = hull_polygon_by_cluster(hull_df, 'cluster')
    hull_points_to_sql(cluster_hulls_dict)
    

if __name__ == '__main__':
    main()
