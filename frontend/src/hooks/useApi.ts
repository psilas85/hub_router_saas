// src/hooks/useApi.ts
import { useEffect, useState } from "react";
import api from "@/services/api";

type HttpMethod = "get" | "post" | "put" | "delete";

interface UseApiOptions {
    method?: HttpMethod;
    url: string;
    body?: any;
    params?: Record<string, any>;
    auto?: boolean; // se false, só executa manualmente via execute()
}

export function useApi<T = any>({
    method = "get",
    url,
    body,
    params,
    auto = true,
}: UseApiOptions) {
    const [data, setData] = useState<T | null>(null);
    const [loading, setLoading] = useState<boolean>(false);
    const [error, setError] = useState<any>(null);

    const execute = async (overrideBody?: any) => {
        setLoading(true);
        setError(null);

        try {
            const response = await api.request<T>({
                method,
                url,
                data: overrideBody ?? body,
                params,
            });
            setData(response.data);
            return response.data;
        } catch (err) {
            setError(err);
            throw err;
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        if (auto) {
            execute();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [url, JSON.stringify(params)]);

    return { data, loading, error, execute };
}
