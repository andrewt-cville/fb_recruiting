from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

app = Dash(__name__)

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = ["Five Stars", "Four Stars", "Three Stars", "Unranked", "Drafted", "Undrafted", "NFL Game Participation"],
      color = "blue"
    ),
    link = dict(
      source = [0, 1, 2, 3, 0, 1, 2, 3, 4], # indices correspond to labels, eg A1, A2, A1, B1, ...
      target = [4, 4, 4, 4, 5, 5, 5, 5, 6],
      value = [1, 6, 18, 5, 3, 33, 184, 259, 29]
  ))])

fig.update_layout(title_text="Maryland Recruits 2002-2017", font_size=24)
fig.show()

if __name__ == '__main__':
    app.run_server(debug=True)