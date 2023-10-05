from paginationapp.models import Pagination
from rest_framework import serializers


class Page_serial( serializers.ModelSerializer ):
    class Meta:
        model = Pagination
        fields = '__all__'



