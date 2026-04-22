//hub_router_1.0.1/frontend/src/components/maps/GeocodeResultMap.tsx

"use client"

import { MapContainer, TileLayer, CircleMarker, Popup } from "react-leaflet"
import { useEffect } from "react"
import { useMap } from "react-leaflet"
import L from "leaflet"
import "leaflet/dist/leaflet.css"

type Ponto = {
    lat: number
    lon: number
    cidade?: string
    setor?: string
    endereco?: string
    cte?: string
}

function FitBounds({ pontos }: { pontos: Ponto[] }) {
    const map = useMap()

    useEffect(() => {
        if (!pontos?.length) return

        const bounds = L.latLngBounds(
            pontos.map(p => [p.lat, p.lon] as [number, number])
        )

        map.fitBounds(bounds, { padding: [40, 40] })

    }, [pontos, map])

    return null
}

export default function GeocodeResultMap({ pontos }: { pontos: Ponto[] }) {

    if (!pontos?.length) return null

    // 🔒 limite segurança
    let pontosMapa = pontos

    if (pontos.length > 1000) {
        const shuffled = [...pontos].sort(() => 0.5 - Math.random())
        pontosMapa = shuffled.slice(0, 1000)
    }

    const center: [number, number] = [
        pontosMapa[0].lat,
        pontosMapa[0].lon
    ]

    return (
        <div className="h-[500px] w-full border rounded">

            <MapContainer
                center={center}
                zoom={5}
                style={{ height: "100%", width: "100%" }}
            >

                <FitBounds pontos={pontosMapa} />

                <TileLayer
                    attribution="OpenStreetMap"
                    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                />

                {pontosMapa.map((p, i) => (

                    <CircleMarker
                        key={i}
                        center={[p.lat, p.lon]}
                        radius={4}
                        pathOptions={{
                            color: "#2563eb",
                            fillOpacity: 0.8
                        }}
                    >

                        <Popup>

                            {p.cte && (
                                <div><b>CTE:</b> {p.cte}</div>
                            )}

                            {p.cidade && (
                                <div><b>Cidade:</b> {p.cidade}</div>
                            )}

                            {p.endereco && (
                                <div>{p.endereco}</div>
                            )}

                        </Popup>

                    </CircleMarker>

                ))}

            </MapContainer>

        </div>
    )
}