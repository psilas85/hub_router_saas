declare module "@changey/react-leaflet-markercluster" {
    import { ComponentType } from "react";
    import { LayerGroupProps } from "react-leaflet";

    const MarkerClusterGroup: ComponentType<LayerGroupProps & {
        chunkedLoading?: boolean;
    }>;

    export default MarkerClusterGroup;
}
