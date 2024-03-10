import dash
from dash import dcc, html
import plotly.graph_objs as go
import pandas as pd

# Read data from CSV file
df = pd.read_csv('movie_data.csv')

# Calculate average rating for each year
average_ratings = df.groupby('Year')['Rating'].mean()
average_ratings = average_ratings.round(2)
sorted_df = df.sort_values(by=['Rating', 'Number of Raters'], ascending=[False, False])
top_bottom_movies = pd.concat([sorted_df.groupby('Country').head(5), sorted_df.groupby('Country').tail(5)])

average_ratings.index = average_ratings.index.astype(str)  # Convert index to string for x-axis
df['Number of Raters'] = df['Number of Raters'].round(3)
app = dash.Dash(__name__, external_stylesheets=['https://fonts.googleapis.com/css2?family=Coming+Soon&display=swap'])

app.layout = html.Div([
    dcc.Graph(
        id='scatter-plot',
        figure={
            'data': [
                go.Scatter(
                    x=df[df['Country'] == country]['Number of Raters'],
                    y=df[df['Country'] == country]['Rating'],
                    mode='markers',
                    marker=dict(
                        size=10,
                        opacity=0.5
                    ),
                    text=df[df['Country'] == country]['English Title'],
                    name=country
                )
                for country in df['Country'].unique()
            ],
            'layout': go.Layout(
                title='Scatter Plot of Number of Raters vs Rating',
                titlefont=dict(family="Coming Soon"),
                xaxis=dict(title='Number of Raters', tickfont=dict(family="Coming Soon")),
                yaxis=dict(title='Rating', tickfont=dict(family="Coming Soon")),
                font=dict(family="Coming Soon")
            )
        }
    ),
    dcc.Graph(
        id='line-chart',
        figure={
            'data': [
                go.Scatter(
                    x=average_ratings.index,
                    y=average_ratings.values,
                    mode='lines',
                    line=dict(width=2),
                    name='Average Rating'
                )
            ],
            'layout': go.Layout(
                title='Average Rating Over the Years',
                titlefont=dict(family="Coming Soon"),
                xaxis=dict(title='Year', tickfont=dict(family="Coming Soon")),
                yaxis=dict(title='Average Rating', tickfont=dict(family="Coming Soon")),
                font=dict(family="Coming Soon")
            )
        }
    ),
    dcc.Graph(
        id='scatter-plot-2',
        figure={
            'data': [
                go.Scatter(
                    x=top_bottom_movies[top_bottom_movies['Country'] == country]['Number of Raters'],
                    y=top_bottom_movies[top_bottom_movies['Country'] == country]['Rating'],
                    mode='markers',
                    marker=dict(
                        size=10,
                        opacity=0.5
                    ),
                    text=top_bottom_movies[top_bottom_movies['Country'] == country]['English Title'],
                    name=country
                )
                for country in top_bottom_movies['Country'].unique()
            ],
            'layout': go.Layout(
                title='Scatter Plot of Highest vs. Lowest Rated Dramas for Each Country',
                titlefont=dict(family="Coming Soon"),
                xaxis=dict(title='Number of Raters', tickfont=dict(family="Coming Soon")),
                yaxis=dict(title='Rating', tickfont=dict(family="Coming Soon")),
                font=dict(family="Coming Soon")
            )
        }
    ),
    dcc.Graph(
        id='episode-rating-scatter',
        figure={
            'data': [
                go.Scatter(
                    x=df['Number of Episode'],
                    y=df['Rating'],
                    mode='markers',
                    marker=dict(
                        size=10,
                        opacity=0.5,
                        color=df['Number of Episode'],
                        colorscale='phase',
                        colorbar=dict(title='Number of Episodes'),
                    ),
                    text=df['English Title'],
                    hoverinfo='text',
                    name='TV Shows'
                )
            ],
            'layout': go.Layout(
                title='Number of Episodes vs. Rating',
                titlefont=dict(family="Coming Soon"),
                xaxis=dict(title='Number of Episodes', tickfont=dict(family="Coming Soon")),
                yaxis=dict(title='Rating', tickfont=dict(family="Coming Soon")),
                font=dict(family="Coming Soon")
            )
        }
    )

])

if __name__ == '__main__':
    app.run_server(debug=True)
