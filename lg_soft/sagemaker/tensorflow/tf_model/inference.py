import io
import json
from collections import namedtuple

import numpy as np
from PIL import Image

Context = namedtuple(
    "Context",
    "model_name, model_version, method, rest_uri, grpc_uri, "
    "custom_attributes, request_content_type, accept_header",
)


def input_handler(data, context):
    """Pre-process request input before it is sent to TensorFlow Serving REST API
    Args:
        data (obj): the request data, in format of dict or string
        context (Context): an object containing request and configuration details
    Returns:
        (dict): a JSON-serializable dict that contains request body and headers
    """

    if context.request_content_type == "application/x-image":

        image_as_bytes = io.BytesIO(data.read())
        image = Image.open(image_as_bytes)
        newsize = (28, 28)
        image = image.resize(newsize)
        instance = np.expand_dims(image, axis=0)
        return json.dumps({"instances": instance.tolist()})

    else:
        _return_error(
            415, 'Unsupported content type "{}"'.format(context.request_content_type or "Unknown")
        )


def output_handler(data, context):
    """Post-process TensorFlow Serving output before it is returned to the client.
    Args:
        data (obj): the TensorFlow serving response
        context (Context): an object containing request and configuration details
    Returns:
        (bytes, string): data to return to client, response content type
    """
    if data.status_code != 200:
        raise Exception(data.content.decode("utf-8"))
    response_content_type = context.accept_header
    prediction = data.content
    return prediction, response_content_type


def _return_error(code, message):
    raise ValueError("Error: {}, {}".format(str(code), message))