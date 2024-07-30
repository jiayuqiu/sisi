import plotly.graph_objects as go

KEYS = ['中心点经纬度', '维度', '经度', '内容']


def get_data():
    city_name = ['北京', '上海', '广州', '深圳', '成都']

    latlngs = [[39.929986, 116.395645],  # 北京中心点经纬度
               [31.249162, 121.487899],  # 上海中心点经纬度
               [23.120049, 113.30765],  # 广州中心点经纬度
               [22.546054, 114.025974],  # 深圳中心点经纬度
               [30.679943, 104.067923]]  # 成都中心点经纬度

    lats = []
    lons = []
    for ll in latlngs:
        lats.append(ll[0])
        lons.append(ll[1])

    ret = {KEYS[0]: latlngs[4],
           KEYS[1]: lats,
           KEYS[2]: lons,
           KEYS[3]: city_name}

    return ret


if __name__ == '__main__':
    # 注册mapbox，获得Access tokens
    # https://account.mapbox.com/
    mapbox_access_token = '注册后获得的mapbox Access token'

    data = get_data()
    print(data)

    center_loc = data[KEYS[0]]
    lats = data[KEYS[1]]
    lons = data[KEYS[2]]
    texts = data[KEYS[3]]

    fig = go.Figure(go.Scattermapbox(
        name='中国城市',
        lat=lats,
        lon=lons,
        mode='markers',
        showlegend=True,
        marker=go.scattermapbox.Marker(
            size=15,
            color='red',
            opacity=0.8,
            symbol='circle'  # 可设置 embassy,marker ,更多在 https://labs.mapbox.com/maki-icons/
        ),
        text=texts,
        textfont=dict(size=18),
    ))

    fig.update_layout(
        hovermode='closest',
        mapbox=dict(
            accesstoken=mapbox_access_token,
            bearing=0,
            center=go.layout.mapbox.Center(
                lat=center_loc[0],
                lon=center_loc[1]
            ),
            pitch=0,
            zoom=3,
        )
    )

    fig.show()

# demo
# https://plotly.com/python/scattermapbox/