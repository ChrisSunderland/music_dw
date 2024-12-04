from django.urls import path
from . import views

urlpatterns = [
    path('', views.get_playlists, name='playlists'),
    path('start-date/', views.get_start, name='start-date'),
    path('end-date/', views.get_end, name='end-date'),
    path('placement-display/', views.display_placement_summary, name='placement-display')
]