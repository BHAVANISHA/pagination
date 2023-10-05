# Generated by Django 4.2.4 on 2023-10-04 17:23

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Pagination',
            fields=[
                ('api_id', models.CharField(max_length=19, primary_key=True, serialize=False)),
                ('id', models.BigIntegerField()),
                ('name', models.CharField(blank=True, max_length=100, null=True)),
                ('tagline', models.CharField(blank=True, max_length=255, null=True)),
                ('first_brewed', models.CharField(blank=True, max_length=500, null=True)),
                ('description', models.CharField(blank=True, max_length=1000, null=True)),
                ('image_url', models.CharField(blank=True, max_length=100, null=True)),
                ('abv', models.IntegerField(blank=True, null=True)),
                ('ibu', models.IntegerField(blank=True, null=True)),
                ('target_fg', models.IntegerField(blank=True, null=True)),
                ('target_og', models.IntegerField(blank=True, null=True)),
                ('ebc', models.CharField(blank=True, max_length=100, null=True)),
                ('srm', models.CharField(blank=True, max_length=100, null=True)),
                ('ph', models.FloatField(blank=True, null=True)),
                ('attenuation_level', models.IntegerField(blank=True, null=True)),
                ('volume', models.CharField(blank=True, max_length=255, null=True)),
                ('boil_volume', models.CharField(blank=True, max_length=255, null=True)),
                ('method', models.CharField(blank=True, max_length=1000, null=True)),
                ('ingredients', models.CharField(blank=True, max_length=5000, null=True)),
                ('food_pairing', models.CharField(blank=True, max_length=1000, null=True)),
                ('brewers_tips', models.CharField(blank=True, max_length=1000, null=True)),
                ('contributed_by', models.CharField(blank=True, max_length=1000, null=True)),
            ],
            options={
                'db_table': 'pagination',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='Post_info',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('age', models.IntegerField()),
            ],
        ),
    ]
