"""
Parsers
"""
from django.conf import settings
from django.utils import six
from rest_framework import parsers
from rest_framework.exceptions import ParseError

from . import exceptions, renderers, utils


class JSONParser(parsers.JSONParser):
    """
    A JSON API client will send a payload that looks like this:

        {
            "data": {
                "type": "identities",
                "id": 1,
                "attributes": {
                    "first_name": "John",
                    "last_name": "Coltrane"
                }
            }
        }

    We extract the attributes so that DRF serializers can work as normal.
    """
    media_type = 'application/vnd.api+json'
    renderer_class = renderers.JSONRenderer

    @staticmethod

    def parse_attributes(data):
        attributes = data.get('attributes')
        uses_format_translation = getattr(settings, 'JSON_API_FORMAT_KEYS', False)

        if not attributes:
            return dict()
        elif uses_format_translation:
            # convert back to python/rest_framework's preferred underscore format
            return utils.format_keys(attributes, 'underscore')
        else:
            return attributes

    @staticmethod
    def parse_relationships(data):
        uses_format_translation = getattr(settings, 'JSON_API_FORMAT_KEYS', False)
        relationships = data.get('relationships')

        if not relationships:
            relationships = dict()
        elif uses_format_translation:
            # convert back to python/rest_framework's preferred underscore format
            relationships = utils.format_keys(relationships, 'underscore')

        # Parse the relationships
        parsed_relationships = dict()
        for field_name, field_data in relationships.items():
            field_data = field_data.get('data')
            if isinstance(field_data, dict) or field_data is None:
                parsed_relationships[field_name] = field_data
            elif isinstance(field_data, list):
                parsed_relationships[field_name] = list(relation for relation in field_data)
        return parsed_relationships

    @staticmethod
    def parse_metadata(result, obj=None):
        if obj is None:
            metadata = result.get('meta')
        else:
            metadata = result.get('meta', {}).copy()
            metadata.update(obj.get('meta', {}))
        if metadata:
            return {'_meta': metadata}
        else:
            return {}

    def parse(self, stream, media_type=None, parser_context=None):
        """
        Parses the incoming bytestream as JSON and returns the resulting data
        """
        result = super(JSONParser, self).parse(
            stream, media_type=media_type, parser_context=parser_context
        )

        if not isinstance(result, dict) or 'data' not in result:
            raise ParseError('Received document does not contain primary data')

        data = result.get('data')

        from rest_framework_json_api.views import RelationshipView
        if isinstance(parser_context['view'], RelationshipView):
            # We skip parsing the object as JSONAPI Resource Identifier Object and not a regular
            # Resource Object
            if isinstance(data, list):
                for resource_identifier_object in data:
                    if not (
                        resource_identifier_object.get('id') and
                        resource_identifier_object.get('type')
                    ):
                        raise ParseError(
                            'Received data contains one or more malformed JSONAPI '
                            'Resource Identifier Object(s)'
                        )
            elif not (data.get('id') and data.get('type')):
                raise ParseError('Received data is not a valid JSONAPI Resource Identifier Object')

            return data

        request = parser_context.get('request')

        if isinstance(data, list):
            # This section is a shim to allow JSON API to PUT/POST/PATCH multiple objects at once
            # Check for inconsistencies
            for obj in data:
                if request.method in ('PUT', 'POST', 'PATCH'):
                    resource_name = utils.get_resource_name(
                        parser_context, expand_polymorphic_types=True)
                    if isinstance(resource_name, six.string_types):
                        if obj.get('type') != resource_name:
                            raise exceptions.Conflict(
                                "The resource object's type ({data_type}) is not the type that "
                                "constitute the collection represented by the endpoint "
                                "({resource_type}).".format(
                                    data_type=obj.get('type'),
                                    resource_type=resource_name))
                    else:
                        if obj.get('type') != resource_name:
                            raise exceptions.Conflict(
                                "The resource object's type ({data_type}) is not the type that "
                                "constitute the collection represented by the endpoint "
                                "(one of [{resource_types}]).".format(
                                    data_type=obj.get('type'),
                                    resource_types=", ".join(resource_name)))
            parsed_data = []
            for obj in data:
                parsed_object = {'id': obj.get('id')} if 'id' in obj else {}
                parsed_object['type'] = obj.get('type')
                parsed_object.update(self.parse_attributes(obj))
                parsed_object.update(self.parse_relationships(obj))
                parsed_object.update(self.parse_metadata(result, obj))
                parsed_data.append(parsed_object)
            return parsed_data
        else:
            # Check for inconsistencies
            if request.method in ('PUT', 'POST', 'PATCH'):
                resource_name = utils.get_resource_name(
                    parser_context, expand_polymorphic_types=True)
                if isinstance(resource_name, six.string_types):
                    if data.get('type') != resource_name:
                        raise exceptions.Conflict(
                            "The resource object's type ({data_type}) is not the type that "
                            "constitute the collection represented by the endpoint "
                            "({resource_type}).".format(
                                data_type=data.get('type'),
                                resource_type=resource_name))
                else:
                    if data.get('type') != resource_name:
                        raise exceptions.Conflict(
                            "The resource object's type ({data_type}) is not the type that "
                            "constitute the collection represented by the endpoint "
                            "(one of [{resource_types}]).".format(
                                data_type=data.get('type'),
                                resource_types=", ".join(resource_name)))
            # Construct the return data
            parsed_data = {'id': data.get('id')} if 'id' in data else {}
            parsed_data['type'] = data.get('type')
            parsed_data.update(self.parse_attributes(data))
            parsed_data.update(self.parse_relationships(data))
            parsed_data.update(self.parse_metadata(result))
            return parsed_data
