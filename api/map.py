
from ipyleaflet import AwesomeIcon, basemaps, Choropleth, Circle, FullScreenControl, GeoJSON, Heatmap, \
    Icon, ImageOverlay, LayersControl, LayerGroup, Marker, Map, Popup, Rectangle, WidgetControl
from ipywidgets import HTML, Accordion
from branca.colormap import linear
import os
from PIL import Image
import numpy as np


class MMLabsMap:
    def __init__(self, data, process_uuid):
        self.data = data
        self.process_uuid = process_uuid
        self.pixel_factor = 0.0
        self.beacons = []
        self.region_geojson = {}
        self.info_html_widget = None
        self.heatmap_data = None
        self.heatmap_group = None
        self.calculated_region_coordinates = {}
        self.calculated_beacon_coordinates = {}
        self.m = None

        self.create_map()
        self.create_infobox()
        self.create_region_layer()
        self.create_beacon_layer()

    def create_infobox(self):
        info_widgets = []
        self.info_html_widget = HTML(value="- Keine Info -")
        info_widgets.append(self.info_html_widget)

        accordion = Accordion(children=info_widgets)
        accordion.set_title(0, 'Info')
        widget_control = WidgetControl(widget=accordion, position='bottomright')
        self.m.add_control(widget_control)

    def calc_pixel_factor(self, layout_image, layout_bounds_tl, layout_bounds_br):
        factor_pixel_y = (layout_bounds_br[0] - layout_bounds_tl[0])
        factor_pixel_x = (layout_bounds_br[1] - layout_bounds_tl[1])
        return factor_pixel_x, factor_pixel_y

    def calc_beacon_geo(self, beaconcoords):
        layout_bounds_tl = self.layout_bounds[0]
        lng = layout_bounds_tl[1] + (self.pixel_factor[0] * beaconcoords[0])
        lat = layout_bounds_tl[0] + (self.pixel_factor[1] * beaconcoords[1])
        return (lat, lng)

    def calc_center(self, tl, br):
        return ((tl[0] + br[0]) / 2, (tl[1] + br[1]) / 2)

    def create_beacon_layer(self):
        """
        creates layer with beacons, initialize beacon array with geocoordinates
        """
        # Create layer group
        beacon_group = LayerGroup(name="Beacons")
        self.m.add_layer(beacon_group)

        icon = AwesomeIcon(
            name='bluetooth',
            marker_color='blue',
            icon_color='darkblue',
            spin=False
        )

        for beacon in self.data.processes[self.process_uuid]["layout"]["beacons"]:
            beacon_coordinates = (beacon["position_x"], beacon["position_y"])
            geocoords = self.calc_beacon_geo(beacon_coordinates)

            marker = Marker(
                location=geocoords,
                icon=icon,
                draggable=False,
                title=str(beacon["id"]) + ": " + str(beacon["comment"]))

            self.beacons.append(geocoords)
            beacon_group.add_layer(marker)

    def create_geojson_for_regions(self, region_ids):
        """ create GeoJSON out of list of region ids """
        geojson_features = []

        for region in region_ids:
            # get geocoordinates
            (tl, br) = self.calculated_region_coordinates[int(region["id"])]

            geocoords = [
                [tl[1], tl[0]],
                [tl[1], br[0]],
                [br[1], br[0]],
                [br[1], tl[0]],
                [tl[1], tl[0]]
            ]

            feature = {
                "type": "Feature",
                "id": str(region["id"]),
                "properties": {
                    "id": str(region["id"]),
                    "name": str(region["name"]),
                    "uuid": str(region["uuid"])
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [geocoords]
                }
            }
            geojson_features.append(feature)

        # return the final GeoJSON
        return {
            "type": "FeatureCollection",
            "features": geojson_features
        }

    def create_region_layer(self):
        """ create region layer with empty data """
        # Create layer group
        region_group = LayerGroup(name="Regionen")
        # self.region_data = {}

        regions = self.data.processes[self.process_uuid]["layout"]["regions"]

        for region in regions:
            # set initial data of all regions to 0 for choropleth
            # self.region_data[str(region)] = 0.0

            coords = (region["position_top_left_x"], region["position_top_left_y"], region["position_bottom_right_x"],
                      region["position_bottom_right_y"])
            tl = self.calc_beacon_geo(coords[:2])
            br = self.calc_beacon_geo(coords[2:])
            # save to global map for later reuse
            self.calculated_region_coordinates[int(region["id"])] = (tl, br)

        self.region_geojson = self.create_geojson_for_regions(regions)

        layer_regions = GeoJSON(
            data=self.region_geojson,
            border_color='black',
            style={'color': 'blue', 'weight': 1.0, 'fillOpacity': 0.1, 'dashArray': '5, 5'},
            hover_style={'color': 'green', 'fillColor': '#ff0000', 'fillOpacity': 0.3, 'weight': 1.5, 'dashArray': '1'}
        )
        layer_regions.on_hover(self.region_hover_handler)
        region_group.add_layer(layer_regions)
        self.m.add_layer(region_group)

    def set_heatmap(self, data=None):
        if self.heatmap_group is None:
            # Create layer group
            self.heatmap_group = LayerGroup(name="Heatmap")
            self.m.add_layer(self.heatmap_group)
        else:
            self.heatmap_group.clear_layers()
        self.heatmap_data = data

        if data is None:
            return

        layer_heatmap = Choropleth(
            geo_data=self.region_geojson,
            choro_data=data,
            colormap=linear.YlOrRd_04.scale(0.0, 1.0),
            # colormap=linear.Blues_09.scale(0.0,  1.0),
            min_value=0.0,
            max_value=1.0,
            border_color='black',
            style={'fillOpacity': 0.8, 'dashArray': '5, 5'})

        layer_heatmap.on_hover(self.heatmap_region_hover_handler)
        self.heatmap_group.add_layer(layer_heatmap)

    def heatmap_region_hover_handler(self, event, feature, properties, id):
        if self.heatmap_data is None:
            self.region_hover_handler(event, feature, properties, id)
            return

        text = str("<b>" + properties["name"] + " (id: " + properties["id"] + ")</b><br/>UUID: " + properties["uuid"] +
                   "<br/>Value: " + str(self.heatmap_data[properties["id"]]))
        self.info_html_widget.value = text

    def region_hover_handler(self, event, feature, properties, id):
        text = str("<b>" + properties["name"] + " (id: " + properties["id"] + ")</b><br/>UUID: " + properties["uuid"])
        self.info_html_widget.value = text

    def create_map(self):
        layout_image = Image.open(self.data.get_layout_image_path(self.process_uuid))
        aspect_ratio = layout_image.width / layout_image.height
        min_lat = self.data.processes[self.process_uuid]["layout"]["lat"] + 0.01
        min_lon = self.data.processes[self.process_uuid]["layout"]["lng"] - 0.01
        max_lon = self.data.processes[self.process_uuid]["layout"]["lng"] + 0.01
        # compute extent of image in lat/lon
        delta_lat = (max_lon - min_lon) / aspect_ratio * np.cos(min_lat / 360 * 2 * np.pi)
        max_lat = min_lat - delta_lat
        latlng_bounds_tl = (min_lat, min_lon)
        latlng_bounds_br = (max_lat, max_lon)
        center = self.calc_center(latlng_bounds_tl, latlng_bounds_br)
        self.layout_bounds = (latlng_bounds_tl, latlng_bounds_br)

        self.pixel_factor = self.calc_pixel_factor(layout_image, latlng_bounds_tl, latlng_bounds_br)

        # create map
        self.m = Map(center=center,
                     zoom=16,
                     basemap=basemaps.OpenStreetMap.Mapnik,
                     scroll_wheel_zoom=True)

        # add the image to the map
        image_path = self.data.get_layout_image_path(self.process_uuid, absolute=True)

        layout_image_map = ImageOverlay(
            url="/files/" + image_path,
            bounds=(latlng_bounds_tl, latlng_bounds_br),
            name="Layout " + self.data.processes[self.process_uuid]["layout"]["name"]
        )
        self.m.add_layer(layout_image_map)

        # add grayscale image to the map
        image_path = os.path.dirname(image_path) + "/layout_gray.png"

        layout_image_map_gray = ImageOverlay(
            url="/files/" + image_path,
            bounds=(latlng_bounds_tl, latlng_bounds_br),
            name="Layout " + self.data.processes[self.process_uuid]["layout"]["name"] + " (gray)"
        )
        self.m.add_layer(layout_image_map_gray)

        control = FullScreenControl()
        self.m.add_control(control)

        control = LayersControl(position='topright')
        self.m.add_control(control)

    def get_map(self):
        return self.m
