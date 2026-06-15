"""Plotting functions copied from EPWpy: https://gitlab.com/epwpy/epwpy/-/blob/develop/EPWpy/plotting/plot_supercond.py?ref_type=heads.

The code is adapted to AiiDA datatypes.
"""

import os

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as numpy
from aiida import orm
from scipy.optimize import curve_fit

from aiida_epw.data import GapFunctionData
from aiida_epw.tools.calculators import bcs_gap_function


def plot_max_eigenvalue(temps, evs, ax=None, **kwargs):
    """Plot the isotropic gap (Imaginary) vs. temeprature."""
    import numpy

    xlim = kwargs.pop("xlim", None)
    ylim = kwargs.pop("ylim", None)
    title = kwargs.pop(
        "title",
        "Max. eigenvalue of linearized Eliashberg equation",
    )
    xlabel = kwargs.pop("xlabel", r"Temeperature (K)")
    ylabel = kwargs.pop("ylabel", r"Max. eigenvalue")

    ##Plot
    if not ax:
        import matplotlib.pyplot as plt

        plt.rcParams.update({"font.size": kwargs.pop("fontsize", 12)})
        plt.rcParams["font.family"] = "serif"
        plt.rcParams["font.serif"] = ["STIXGeneral"]
        plt.rcParams["mathtext.fontset"] = "stix"
        plt.rcParams["font.family"] = "STIXGeneral"
        plt.rcParams["mathtext.default"] = "regular"
        fig, axs = plt.subplots(
            1, 1, figsize=(5, 4), squeeze=False, constrained_layout=True
        )
        ax = axs[0, 0]

    ax.set_title(title)
    ax.set_xlim([numpy.min(temps), numpy.max(temps)] if not xlim else xlim)
    ax.set_ylim([numpy.min(evs), numpy.max(evs)] if not ylim else ylim)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.tick_params(axis="y")
    ax.tick_params(axis="x")
    ax.plot(
        temps,
        evs,
        linestyle="-",
        marker="o",
        c="k",
    )

    ax.axhline(1.0, ls="--", c="gray", lw=1.5)
    # ax.yaxis.set_minor_locator(ticker.AutoMinorLocator(2))


def _iter_gap_functions(gap_functions):
    """Yield `(temperature, table)` pairs from typed or legacy gap-function data."""
    if isinstance(gap_functions, GapFunctionData):
        yield from gap_functions.get_itergap_functions()
        return

    for arrayname, array in gap_functions.get_iterarrays():
        yield float(arrayname.replace("_", ".")), array


#### Isotropic gap (Imaginary, real and ) vs. temeprature


def gap_iso_imag_temp(
    iso_gap_function: orm.ArrayData,
    tempmax,
    font=12,
    prefix="aiida",
    fit=False,
    p0=None,
    destpath=None,
):
    """Plot the isotropic gap (Imaginary) vs. temeprature."""
    imag_delta = []
    imag_temp = []

    for temperature, array in _iter_gap_functions(iso_gap_function):
        gap = array[0, -1] * 1000
        if numpy.isnan(gap):
            continue
        imag_delta.append(gap)  # Convert to meV
        imag_temp.append(temperature)

    ##Plot
    fig = plt.figure(figsize=(4.5, 3.5))
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.set_title("Superconducting Gap vs. Temperature", fontsize=font)
    ax1.set_xlabel("Temeperature (K)", fontsize=font)
    ax1.set_xlim(0, tempmax)
    ax1.set_ylabel(r"$\Delta_0$ (meV)", fontsize=font)
    ax1.tick_params(axis="y", labelsize=font)
    ax1.tick_params(axis="x", labelsize=font)
    ax1.plot(
        imag_temp,
        imag_delta,
        linestyle="-",
        marker="o",
        c="k",
        label="Im. axis",
    )
    ax1.yaxis.set_minor_locator(ticker.AutoMinorLocator(2))
    plt.tight_layout()
    if fit:
        if p0 is None:
            p0 = [imag_temp[-1], 3.3, imag_delta[0]]
        popt, pcov = curve_fit(bcs_gap_function, imag_temp, imag_delta, p0=p0)
        Tc, p, Delta_0 = popt
        T = numpy.linspace(0, Tc, 100)
        ax1.plot(
            T,
            bcs_gap_function(T, Tc, p, Delta_0),
            linestyle="--",
            c="r",
            label="Fit",
        )
    if destpath:
        plt.savefig(os.path.join(destpath, f"{prefix}_iso_gap_imag_vs_Temp.pdf"))
    plt.show()


def fitting_function(T, p, delta_zero, Tc):
    """
    Standard BCS-like fitting function for superconducting gap.
    Handles T > Tc safely by returning 0.
    """
    T = numpy.atleast_1d(T)
    gap = numpy.zeros_like(T, dtype=float)
    mask = T < Tc
    gap[mask] = delta_zero * (1.0 - (T[mask] / Tc) ** p) ** 0.5
    return gap if len(gap) > 1 else gap[0]


def find_multigap_averages(data, T, bandwidth_factor=1.5):
    """
    使用自适应核密度估计 (KDE) 寻找能隙峰。
    基于统计学自适应带宽，不限制任何峰的个数或物理距离。
    """
    from scipy.signal import find_peaks
    from sklearn.neighbors import KernelDensity

    gaps = data[:, 1]
    base_value = numpy.min(data[:, 0])
    signal = data[:, 0] - base_value

    # 1. 将直方图展开为一维点集
    max_sig = numpy.max(signal)
    if max_sig == 0:
        return [numpy.mean(gaps)]

    virtual_samples = []
    for g, w in zip(gaps, (signal / max_sig * 100).astype(int)):
        if w > 0:
            virtual_samples.extend([g] * w)
    X = numpy.array(virtual_samples)

    if len(X) < 10:
        return [numpy.mean(gaps)]

    # 2. 【核心步骤】：使用 Silverman 拇指法则计算统计学自适应带宽
    std_dev = numpy.std(X)
    n_samples = len(X)
    # 标准 Silverman 公式: 1.06 * std * n**(-1/5)
    silverman_bw = 1.06 * std_dev * (n_samples ** (-0.2))

    # 稍微放大带宽因子（比如 1.5），用于把靠得极近的“双肩精细结构”融合成单峰
    adaptive_bw = silverman_bw * bandwidth_factor

    # 3. 拟合 KDE 曲线
    kde = KernelDensity(kernel="gaussian", bandwidth=adaptive_bw).fit(X.reshape(-1, 1))

    # 4. 在全能量区间上对拟合出的平滑曲线进行采样
    x_eval = numpy.linspace(numpy.min(gaps), numpy.max(gaps), 300)
    log_dens = kde.score_samples(x_eval.reshape(-1, 1))
    density = numpy.exp(log_dens)

    # 5. 寻找平滑曲线上的局部极大值
    # 因为曲线已经被统计学带宽自然平滑，不需要再加 distance 限制
    peaks, _ = find_peaks(density, prominence=numpy.max(density) * 0.1)

    if len(peaks) == 0:
        return [numpy.mean(gaps)]

    return sorted(x_eval[peaks].tolist())


def plot_anisotropic_gap(
    aniso_gap_functions_dict,
    ax=None,
    fit=True,
    p0=None,
    destpath=None,
    **kwargs,
):
    """Plot the anisotropic gap vs. temperature and fit multi-gap functions automatically."""
    from scipy.optimize import curve_fit

    xlim = kwargs.pop("xlim", None)
    ylim = kwargs.pop("ylim", None)
    title = kwargs.pop("title", "Multi-gap Fitting Analysis")
    xlabel = kwargs.pop("xlabel", r"Temperature (K)")
    ylabel = kwargs.pop("ylabel", r"$\Delta_{nk}$ (meV)")

    if not ax:
        import matplotlib.pyplot as plt

        plt.rcParams.update({"font.size": kwargs.pop("fontsize", 12)})
        plt.rcParams["font.family"] = "serif"
        plt.rcParams["font.serif"] = ["STIXGeneral"]
        plt.rcParams["mathtext.fontset"] = "stix"
        plt.rcParams["font.family"] = "STIXGeneral"
        plt.rcParams["mathtext.default"] = "regular"
        fig, axs = plt.subplots(
            1, 1, figsize=(6, 5), squeeze=False, constrained_layout=True
        )
        ax = axs[0, 0]

    sorted_temps = sorted(aniso_gap_functions_dict.keys())

    # 用字典动态追踪不同的能隙分支：{branch_index: (list_of_T, list_of_delta)}
    branches = {}

    if len(sorted_temps) > 1:
        dT = numpy.mean(numpy.diff(sorted_temps))
    else:
        dT = 3.0

    for T in sorted_temps:
        array = numpy.array(aniso_gap_functions_dict[T])

        # 1. 改为传统的对称直方图（小提琴谱线形式）
        base_value = numpy.min(array[:, 0])
        signal = array[:, 0] - base_value
        max_sig = numpy.max(signal)

        if max_sig > 0:
            # 缩放让直方图半宽最大不超过 dT 的 45%，防止互相严重重叠
            scale = (dT * 0.45) / max_sig
            ax.fill_betweenx(
                y=array[:, 1],
                x1=T - signal * scale,
                x2=T + signal * scale,
                color="tab:blue",
                alpha=0.15,  # 透明度
                edgecolor="tab:blue",  # 直方图轮廓线颜色
                linewidth=0.5,  # 轮廓线粗细
                zorder=1,
            )

        # max_density = numpy.max(array[:, 0])
        # scale = dT * 0.8 / max_density if max_density > 0 else 1.0
        # ax.barh(
        #     y=array[:, 1],
        #     width=array[:, 0] * scale,
        #     left=T,
        #     height=0.03,  # 条形图里每块的高度，对应你数据的能量网格间距（0.027 meV）
        #     color='royalblue',
        #     alpha=0.2,
        #     edgecolor='none',
        #     zorder=1
        # )
        # 2. 自动提取当前温度下的所有能隙中心点
        rep_gaps = find_multigap_averages(
            array,
            T,
            # kwargs.pop("prominence_ratio", 0.1),
            # kwargs.pop("sigma", 2)
        )

        # 3. 将提取出的点分门别类归入各自的分支中（按从小到大的顺序分配索引 0, 1, 2...）
        for idx, vg in enumerate(rep_gaps):
            branches.setdefault(idx, ([], []))
            branches[idx][0].append(T)
            branches[idx][1].append(vg)

        # 绘制提取出来的代表性红点
        if rep_gaps:
            ax.scatter(
                [T] * len(rep_gaps),
                rep_gaps,
                color="red",
                edgecolors="black",
                s=25,
                zorder=5,
            )

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)

    if xlim:
        ax.set_xlim(xlim)
    if ylim:
        ax.set_ylim(ylim)

    # 4. 动态对所有识别出的分支独立进行 BCS 拟合
    if fit:
        # 使用不同的颜色区分拟合曲线
        color_cycle = ["r", "g", "b", "m", "c"]

        for b_idx, (ts, ds) in branches.items():
            ts = numpy.array(ts)
            ds = numpy.array(ds)

            # 如果某个分支的数据点太少（例如高温下某些小能隙闭合了），则跳过拟合
            if len(ts) < 3:
                continue

            # 为当前分支做初始猜测 [p, delta_zero, Tc]
            if p0 is None:
                current_p0 = [3.0, ds[0], ts[-1] * 1.05]
            else:
                current_p0 = p0

            try:
                # 施加合理的物理边界：p在0.5~10之间，Delta_0大于0，Tc大于当前观测最高温度
                popt, pcov = curve_fit(
                    fitting_function,
                    ts,
                    ds,
                    p0=current_p0,
                    maxfev=10000,
                    bounds=((0.5, 0.0, ts[-1]), (10.0, ds[0] * 2, ts[-1] * 2)),
                )
                p_fit, delta_zero_fit, Tc_fit = popt

                # 绘制拟合线
                T_fit = numpy.linspace(0, Tc_fit, 100)
                ax.plot(
                    T_fit,
                    fitting_function(T_fit, p_fit, delta_zero_fit, Tc_fit),
                    linestyle="--",
                    color=color_cycle[b_idx % len(color_cycle)],
                    linewidth=2,
                    label=f"$\Delta_0$={delta_zero_fit:.2f}, $T_c$={Tc_fit:.1f} (K)",
                )
            except Exception as e:
                print(f"Branch {b_idx + 1} fitting failed: {e}")

        # ax.legend(loc="upper right")

    if destpath:
        plt.savefig(destpath, dpi=300)
