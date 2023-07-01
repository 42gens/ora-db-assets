"""
URL configuration for oracle_project project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from oracle_app.views import OracleStateView, download_csv, download_txt

urlpatterns = [
    path('', OracleStateView.as_view(), name='oracle_state'),
    path('download_csv/', download_csv, name='csv_content'),
    path('download_txt/', download_txt, name='txt_content'),
]
