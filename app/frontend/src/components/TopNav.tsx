import { NavLink } from "react-router-dom";
import {
  IconLayoutGrid,
  IconChartBar,
  IconStar,
  IconPin,
  IconBriefcase
} from "@tabler/icons-react";

export default function TopNav() {
  return (
    <nav className="top-nav">
      <NavLink
        to="/"
        end
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        <IconLayoutGrid size={16} />
        <span>スクリーナー</span>
      </NavLink>
      <NavLink
        to="/ranking"
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        <IconChartBar size={16} />
        <span>ランキング</span>
      </NavLink>
      <NavLink
        to="/favorites"
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        <IconStar size={16} />
        <span>お気に入り</span>
      </NavLink>
      <NavLink
        to="/candidates"
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        <IconPin size={16} />
        <span>候補</span>
      </NavLink>
      <NavLink
        to="/positions"
        className={({ isActive }) => (isActive ? "top-nav-link active" : "top-nav-link")}
      >
        <IconBriefcase size={16} />
        <span>保有</span>
      </NavLink>
    </nav>
  );
}
