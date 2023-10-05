import concurrent
import json
import requests
import concurrent.futures
import logging
import random
import string
import time
import httpx
from django.db import transaction, connection
from rest_framework import views, status
from rest_framework.response import Response
from asgiref.sync import sync_to_async
from adrf.views import APIView


# PAGINATION
# using thread pool executor method
class Thread_page( views.APIView ):
    def get(self, request):
        start = time.time()

        def insert_data(data):
            with connection.cursor() as cursor:
                api_data = data
                for item in api_data:
                    raw_query = """
                    INSERT INTO pagination (api_id, id, name, tagline, first_brewed, description, image_url, abv, ibu, target_fg, target_og, ebc, srm, ph, attenuation_level, volume, boil_volume, method, ingredients, food_pairing,
 brewers_tips, contributed_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    params = (generate_api_id(),
                              item["id"],
                              item["name"],
                              item["tagline"],
                              item["first_brewed"],
                              item["description"],
                              item["image_url"],
                              item["abv"],
                              item["ibu"],
                              item["target_fg"],
                              item["target_og"],
                              item["ebc"],
                              item["srm"],
                              item["ph"],
                              item["attenuation_level"],
                              str( item["volume"] ),
                              str( item["boil_volume"] ),
                              str( item["method"] ),
                              str( item["ingredients"] ),
                              str( item["food_pairing"] ),
                              item["brewers_tips"],
                              item["contributed_by"]
                              )
                    cursor.execute( raw_query, params )

        def generate_api_id():
            with connection.cursor() as cursor:
                api_id = "API" + "".join( random.choice( string.digits ) for i in range( 6 ) )
                cursor.execute( "SELECT api_id FROM pagination WHERE api_id=%s", (api_id,) )
                db_id = cursor.fetchone()
                if not db_id:
                    return api_id
                else:
                    api_id = generate_api_id()
                    return api_id

        def fetch_data(url):
            response = requests.get( url )
            if response.status_code == 200:
                data = response.json()
                print( data )
                insert_data( data )
                return {"message": "success"}
            return {'error': f'Failed to retrieve data from {url}'}

        api_urls = ['https://api.punkapi.com/v2/beers/',
                    'https://api.punkapi.com/v2/beers/',
                    'https://api.punkapi.com/v2/beers/', ]

        def page(url):
            for i in range( 1, 11 ):
                api_url = str( url ) + str( i )
                fetch_data( api_url )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list( executor.map( page, api_urls ) )
        end = time.time()
        tot = end - start
        val = {"total": tot, "data": "Data inserted successfully"}
        return Response( val, status=status.HTTP_201_CREATED )


# pagination using asyncio

class Async_Pagination( APIView ):

    @staticmethod
    def insert_data_sync(data):
        try:
            with transaction.atomic():
                api_data = data
                for item in api_data:
                    raw_query = """
                    INSERT INTO pagination (
                        api_id, id, name, tagline, first_brewed, description, image_url,
                        abv, ibu, target_fg, target_og, ebc, srm, ph, attenuation_level,
                        volume, boil_volume, method, ingredients, food_pairing,
                        brewers_tips, contributed_by
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    """
                    params = (
                        Async_Pagination.generate_api_id_sync(),
                        item["id"],
                        item["name"],
                        item["tagline"],
                        item["first_brewed"],
                        item["description"],
                        item["image_url"],
                        item["abv"],
                        item["ibu"],
                        item["target_fg"],
                        item["target_og"],
                        item["ebc"],
                        item["srm"],
                        item["ph"],
                        item["attenuation_level"],
                        str( item["volume"] ),
                        str( item["boil_volume"] ),
                        str( item["method"] ),
                        str( item["ingredients"] ),
                        str( item["food_pairing"] ),
                        item["brewers_tips"],
                        item["contributed_by"]
                    )
                    with connection.cursor() as cursor:
                        cursor.execute( raw_query, params )
                transaction.commit()
        except Exception as e:
            logging.error( f'Error while inserting data: {str( e )}' )

    @sync_to_async
    def generate_api_id(self):
        api_id = "API" + "".join( random.choice( string.digits ) for _ in range( 5 ) )
        with connection.cursor() as cursor:
            cursor.execute( "SELECT api_id FROM pagination WHERE api_id=%s", (api_id,) )
            db_id = cursor.fetchone()
            if not db_id:
                return api_id
            else:
                return self.generate_api_id()

    @staticmethod
    def generate_api_id_sync():
        api_id = "API" + "".join( random.choice( string.digits ) for _ in range( 5 ) )
        with connection.cursor() as cursor:
            cursor.execute( "SELECT api_id FROM pagination WHERE api_id=%s", (api_id,) )
            db_id = cursor.fetchone()
            if not db_id:
                return api_id
            else:
                return Async_Pagination.generate_api_id_sync()

    async def fetch_data(self, url):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get( url )
                if response.status_code == 200:
                    data = response.json()
                    self.insert_data_sync( data )
                    return {"message": "success"}
                else:
                    return {'error': f'Failed to retrieve data from {url}'}
        except Exception as e:
            return {'error': f'Error while fetching data from {url}: {str( e )}'}

    async def page(self, url):
        for i in range( 1, 11 ):
            await self.fetch_data( f'{url}{i}' )

    async def get(self, request):
        start = time.time()

        # Configure logging
        logging.basicConfig( level=logging.INFO )

        api_urls = [
            'https://api.punkapi.com/v2/beers/',
            'https://api.punkapi.com/v2/beers/',
            'https://api.punkapi.com/v2/beers/',
        ]

        with connection.cursor() as cursor:
            self.cursor = cursor
            for api_url in api_urls:
                await self.page( api_url )

        end = time.time()
        total_time = end - start
        response_data = {"total": total_time, "data": "Data inserted successfully"}
        return Response( response_data, status=status.HTTP_201_CREATED )


# ===================================================================================================================================


# Normal get method to create token for superuser

from paginationapp import serializers, models
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework import generics
from django.contrib.auth.models import User
from rest_framework.authtoken.models import Token


class Getdata( generics.ListAPIView ):
    # permission_classes = [IsAuthenticated]
    authentication_classes = [TokenAuthentication]
    serializer_class = serializers.Page_serial
    queryset = models.Pagination.objects.all()

    def get(self, request, *args, **kwargs):
        try:
            # user = User.objects.get( username='bhavi' )
            # Token.objects.create( user=user )
            queryset = models.Pagination.objects.all()
            serializer = serializers.Page_serial( queryset, many=True )
            return Response( serializer.data )
        except Exception as e:
            return Response( str( e ) )


# Insert token with data in db
class Insert_token( views.APIView ):
    def get(self, request):
        start = time.time()

        def insert_data(data):
            with connection.cursor() as cursor:
                for item in data:
                    raw_query = """
                                    INSERT INTO paginationapp_insertion(api_id,
                                        id, name, tagline, first_brewed, description, image_url,
                                        abv, ibu, target_fg, target_og, ebc, srm, ph, attenuation_level,
                                        volume, boil_volume, method, ingredients, food_pairing,
                                        brewers_tips, contributed_by
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s
                                    )"""
                    params = (
                        item['api_id'],
                        item["id"],
                        item["name"],
                        item["tagline"],
                        item["first_brewed"],
                        item["description"],
                        item["image_url"],
                        item["abv"],
                        item["ibu"],
                        item["target_fg"],
                        item["target_og"],
                        item["ebc"],
                        item["srm"],
                        item["ph"],
                        item["attenuation_level"],
                        str( item["volume"] ),
                        str( item["boil_volume"] ),
                        str( item["method"] ),
                        str( item["ingredients"] ),
                        str( item["food_pairing"] ),
                        item["brewers_tips"],
                        item["contributed_by"]
                    )

                    cursor.execute( raw_query, params )

        def fetch_data(url):
            try:
                headers = {
                    "Authorization": "Token 5da276c3148c5a097df30f4895f4db6db448f4fa"
                }
                response = requests.get( url, headers=headers )
                response.raise_for_status()  # Raise an exception for non-200 status codes
                data = response.json()
                insert_data( data )
                return {"message": "success"}
            except requests.exceptions.RequestException as e:
                return {'error': f'Failed to retrieve data from {url}: {str( e )}'}
            except json.JSONDecodeError as e:
                return {'error': f'Failed to parse JSON response from {url}: {str( e )}'}

        api_urls = ['http://127.0.0.1:8000/post/',
                    ]
        with concurrent.futures.ThreadPoolExecutor( max_workers=len( api_urls ) ) as executor:
            results = list( executor.map( fetch_data, api_urls ) )
        data = [result for result in results]
        end = time.time()
        tot = end - start
        # val = {"total": tot, "data": "Data inserted successfully"}

        return Response( data, status=status.HTTP_201_CREATED )
