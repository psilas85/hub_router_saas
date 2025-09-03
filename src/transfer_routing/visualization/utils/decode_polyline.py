def decode_polyline(polyline_str):
    index, lat, lng, coordinates = 0, 0, 0, []
    changes = {'latitude': 0, 'longitude': 0}

    while index < len(polyline_str):
        for key in ['latitude', 'longitude']:
            shift = result = 0

            while True:
                b = ord(polyline_str[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break

            if (result & 1):
                changes[key] = ~(result >> 1)
            else:
                changes[key] = (result >> 1)

        lat += changes['latitude']
        lng += changes['longitude']
        coordinates.append((lat / 1e5, lng / 1e5))

    return coordinates
