export function LoadingScreen({ visible }: { visible: boolean }) {
  if (!visible) return null;
  return (
    // <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
    //   <h1>Loading</h1>
    // </div>
    <div className="fixed inset-0 z-50 flex flex-col items-center justify-center bg-black/50 backdrop-blur-sm">
      {/* Spinner */}
      <div className="w-12 h-12 border-6 border-t-transparent border-white rounded-full animate-spin mb-6"></div>

      {/* Message */}
      <div className="text-center text-white px-4">
        <h2 className="text-xl font-semibold mb-2">Running Backtest...</h2>
        <p className="text-sm">
          This could take up to a few minutes. Please do not refresh the page.
        </p>
      </div>
    </div>
  );
}
