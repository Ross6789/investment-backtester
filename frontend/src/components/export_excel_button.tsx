import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";

export const ExcelDownloadButton: React.FC<{
  excel_download: string | null;
}> = ({ excel_download }) => {
  if (!excel_download) return null;

  const handleDownload = async () => {
    console.log("download button clicked");
    try {
      const apiHost = import.meta.env.VITE_API_BASE_URL;
      const fullUrl = `${apiHost}${excel_download}`;
      console.log(fullUrl);

      const response = await fetch(fullUrl);
      if (!response.ok) throw new Error("File not available");

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "Backtest Summary Report"; //excel_download.split("file=")[1]; // get filename
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (error) {
      alert(
        "Download expired. Excel report must be downloaded within 10 minutes of generation."
      );
    }
  };

  return (
    <Button variant="outline" onClick={handleDownload}>
      <Download />
      Export to Excel
    </Button>
  );
};
