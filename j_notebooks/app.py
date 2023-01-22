from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

import pandas as pd

app = Dash(__name__)

df = (pd.read_csv("https://raw.githubusercontent.com/andrewt-cville/fb_recruiting/master/j_notebooks/sankey_output.csv")).set_index('college')

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = ["Five Stars", "Four Stars", "Three Stars", "Two Stars", "Unranked", "Drafted", "Undrafted", "NFL Game Participation"],
      color = "blue"
    ),
    link = dict(
      source = [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6], # indices correspond to labels, eg A1, A2, A1, B1, ...
      target = [5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 7, 7],
      value = (df.loc['maryland']).values.tolist()
  ))])

fig.update_layout(title_text="Maryland Recruits 2002-2017", font_size=24)
fig.show()

if __name__ == '__main__':
    app.run_server(debug=True)