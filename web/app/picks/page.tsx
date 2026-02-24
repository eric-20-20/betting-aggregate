import { redirect } from "next/navigation";
import { getTodayDate, getAvailableDates } from "@/lib/data";

export default async function PicksPage() {
  const today = getTodayDate();
  const dates = await getAvailableDates();
  const targetDate = dates.length > 0 ? dates[0] : today;
  redirect(`/picks/${targetDate}`);
}
