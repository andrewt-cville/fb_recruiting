from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

import pandas as pd


df = (pd.read_csv("https://raw.githubusercontent.com/andrewt-cville/fb_recruiting/master/j_notebooks/sankey_output.csv")).set_index('college')

app = Dash(__name__)

app.layout = html.Div([
    html.H1('College Recruiting Results: 2002 - 2017'),

    dcc.Dropdown(
        df.index.unique(),
        'maryland',
        id='college'),

    dcc.Graph(id='sankey')
])

@app.callback(
    Output('sankey', 'figure'),
    Input('college', 'value')
)

def update_sankey(college):

    fig = go.Figure(data=[go.Sankey(
        node = dict(
        pad = 15,
        thickness = 20,
        line = dict(color = "black", width = 0.5),
        label = ["Five Stars", "Four Stars", "Three Stars", "Two Stars", "Unranked", "Drafted", "Undrafted"],
        color = "blue"
        ),
        link = dict(
        source = [0, 1, 2, 3, 4, 0, 1, 2, 3, 4], # indices correspond to labels, eg A1, A2, A1, B1, ...
        target = [5, 5, 5, 5, 5, 6, 6, 6, 6, 6],
        value = (df.loc[college]).values.tolist()
    ))])

    fig.update_layout(font_size=24)

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)