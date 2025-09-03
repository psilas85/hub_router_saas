import Navbar from "@/components/Navbar";

export default function ProtectedLayout({ children }: { children: React.ReactNode }) {
    return (
        <div className="flex flex-col h-screen">
            <Navbar />
            <main className="flex-1 overflow-y-auto bg-gray-50 p-6">{children}</main>
        </div>
    );
}
