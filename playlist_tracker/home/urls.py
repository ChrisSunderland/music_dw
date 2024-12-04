from django.urls import path
from . import views

# URLConf
urlpatterns = [
    path('', views.load_home, name='home'),
    path('track-playlist/', views.track_playlist, name='track-playlist')
]
