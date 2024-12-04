from django.urls import path
from . import views

urlpatterns = [
    path('', views.labels, name='labels'),
    path('artists/', views.artists, name='artists'),
    path('tracks/', views.releases, name='tracks'),
    path('track-plot/', views.display_plots, name='track-plot')
]