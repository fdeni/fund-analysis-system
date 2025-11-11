"""
Fund metrics calculator service
"""
from typing import Dict, Any, Optional
from decimal import Decimal
import numpy as np
import numpy_financial as npf
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.models.transaction import CapitalCall, Distribution, Adjustment


class MetricsCalculator:
    """Calculate fund performance metrics such as PIC, DPI, IRR, NAV, RVPI, and TVPI."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def calculate_all_metrics(self, fund_id: int) -> Dict[str, Any]:
        """Compute all key performance metrics for a given fund."""
        pic = self.calculate_pic(fund_id)
        total_distributions = self.calculate_total_distributions(fund_id)
        dpi = self.calculate_dpi(fund_id)
        irr = self.calculate_irr(fund_id)
        nav = self.calculate_nav(fund_id)
        rvpi = self.calculate_rvpi(fund_id)
        tvpi = self.calculate_tvpi(fund_id)        
        
        return {
            "pic": float(pic) if pic else 0,
            "total_distributions": float(total_distributions) if total_distributions else 0,
            "dpi": float(dpi) if dpi else 0,
            "irr": float(irr) if irr else 0,
            "nav": float(nav) if nav else 0,
            "rvpi": float(rvpi) if rvpi else 0,
            "tvpi": float(tvpi) if tvpi else 0,
        }
    
    def calculate_pic(self, fund_id: int) -> Optional[Decimal]:
        """
        Calculate Paid-In Capital (PIC)
        PIC = Total Capital Calls - Adjustments
        """
        # Get total capital calls
        total_calls = self.db.query(
            func.sum(CapitalCall.amount)
        ).filter(
            CapitalCall.fund_id == fund_id
        ).scalar() or Decimal(0)
        
        # Get total adjustments
        total_adjustments = self.db.query(
            func.sum(Adjustment.amount)
        ).filter(
            Adjustment.fund_id == fund_id
        ).scalar() or Decimal(0)
        
        # Compute PIC as total calls minus adjustments
        pic = total_calls - total_adjustments
        return pic if pic > 0 else Decimal(0)
    
    def calculate_total_distributions(self, fund_id: int) -> Optional[Decimal]:
        """
        Calculate the total amount of distributions for a given fund.
        Returns the sum of all distribution amounts or 0 if none exist.
        """
        # Sum all distribution amounts for the specified fund
        total = self.db.query(
            func.sum(Distribution.amount)
        ).filter(
            Distribution.fund_id == fund_id
        ).scalar() or Decimal(0)
        
        # Return total distributions as Decimal
        return total
    
    def calculate_dpi(self, fund_id: int) -> Optional[float]:
        """
        Calculate DPI (Distribution to Paid-In)
        DPI = Cumulative Distributions / PIC
        """
        # Get total paid-in capital
        pic = self.calculate_pic(fund_id)
        # Get total distributions
        total_distributions = self.calculate_total_distributions(fund_id)
        
        # Avoid division by zero
        if not pic or pic == 0:
            return 0.0
        
        # Calculate DPI ratio and round to 4 decimal places
        dpi = float(total_distributions) / float(pic)
        return round(dpi, 4)
    
    def calculate_irr(self, fund_id: int) -> Optional[float]:
        """
        Calculate IRR (Internal Rate of Return)
        Uses numpy-financial's irr function
        """
        try:
            # Get all cash flows sorted by date
            cash_flows = self._get_cash_flows(fund_id)
            
            if len(cash_flows) < 2:
                return None
            
            # Extract amounts
            amounts = [cf['amount'] for cf in cash_flows]
            
            # Calculate IRR (returns as decimal, e.g., 0.15 for 15%)
            irr = npf.irr(amounts)
            
            if irr is None or np.isnan(irr) or np.isinf(irr):
                return None
            
            # Convert to percentage
            return round(float(irr) * 100, 2)
            
        except Exception as e:
            print(f"Error calculating IRR: {e}")
            return None
        
    def calculate_nav(self, fund_id: int) -> Optional[float]:
        """
        Calculate NAV (Net Asset Value)
        NAV represents the current unrealized value of fund investments.
        For simplicity, we use any Adjustment entries tagged as 'NAV_ADJUSTMENT'.
        """
        try:
            # Sum all adjustment amounts labeled as 'NAV_ADJUSTMENT' for the given fund
            nav_value = self.db.query(func.sum(Adjustment.amount)).filter(
                Adjustment.fund_id == fund_id,
                Adjustment.adjustment_type == "NAV_ADJUSTMENT"
            ).scalar() or Decimal(0)
            
            # Return NAV as a float value
            return float(nav_value)
        except Exception as e:
            # Log or print any unexpected database or conversion errors
            print(f"Error calculating NAV: {e}")
            return None


    def calculate_rvpi(self, fund_id: int) -> Optional[float]:
        """
        Calculate RVPI (Residual Value to Paid-In)
        RVPI = NAV / PIC
        Shows the unrealized value remaining in the portfolio.
        """
        try:
            # Get current Net Asset Value (NAV)
            nav = self.calculate_nav(fund_id)
            # Get total Paid-In Capital (PIC)
            pic = self.calculate_pic(fund_id)
            
            # Avoid division by zero
            if not pic or pic == 0:
                return 0.0
            
            # Calculate RVPI ratio and round to 4 decimal places
            rvpi = float(nav) / float(pic)
            return round(rvpi, 4)
        except Exception as e:
            # Log or print any unexpected or conversion errors
            print(f"Error calculating RVPI: {e}")
            return None


    def calculate_tvpi(self, fund_id: int) -> Optional[float]:
        """
        Calculate TVPI (Total Value to Paid-In)
        TVPI = (Distributions + NAV) / PIC
        Combines realized and unrealized gains to show total fund performance.
        """
        try:
            # Get total realized distributions
            total_distributions = self.calculate_total_distributions(fund_id)
            # Get current unrealized Net Asset Value (NAV)
            nav = self.calculate_nav(fund_id)
            # Get total Paid-In Capital (PIC)
            pic = self.calculate_pic(fund_id)

            # Avoid division by zero
            if not pic or pic == 0:
                return 0.0
            
            # Calculate TVPI ratio and round to 4 decimal places
            tvpi = (float(total_distributions) + float(nav)) / float(pic)
            return round(tvpi, 4)
        except Exception as e:
            # Log or print any errors during calculation
            print(f"Error calculating TVPI: {e}")
            return None

    
    def _get_cash_flows(self, fund_id: int) -> list:
        """
        Get all cash flows for IRR calculation
        Capital calls are negative, distributions are positive
        """
        cash_flows = []
        
        # Get capital calls (negative cash flows)
        calls = self.db.query(
            CapitalCall.call_date,
            CapitalCall.amount
        ).filter(
            CapitalCall.fund_id == fund_id
        ).order_by(
            CapitalCall.call_date
        ).all()
        
        for call in calls:
            cash_flows.append({
                'date': call.call_date,
                'amount': -float(call.amount),  # Negative for outflow
                'type': 'capital_call'
            })
        
        # Get distributions (positive cash flows)
        distributions = self.db.query(
            Distribution.distribution_date,
            Distribution.amount
        ).filter(
            Distribution.fund_id == fund_id
        ).order_by(
            Distribution.distribution_date
        ).all()
        
        for dist in distributions:
            cash_flows.append({
                'date': dist.distribution_date,
                'amount': float(dist.amount),  # Positive for inflow
                'type': 'distribution'
            })
        
        # Sort by date
        cash_flows.sort(key=lambda x: x['date'])
        
        return cash_flows
    
    def get_calculation_breakdown(self, fund_id: int, metric: str) -> Dict[str, Any]:
        """
        Get detailed breakdown of a calculation with cash flows for debugging
        
        Args:
            fund_id: Fund ID
            metric: Metric name (dpi, irr, pic)
            
        Returns:
            Detailed breakdown with intermediate values and transaction details
        """
        # DPI (Distribution to Paid-In) Breakdown
        if metric == "dpi":
            pic = self.calculate_pic(fund_id)
            total_distributions = self.calculate_total_distributions(fund_id)
            dpi = self.calculate_dpi(fund_id)
            
            # Fetch related transactions for transparency
            capital_calls = self.db.query(CapitalCall).filter(
                CapitalCall.fund_id == fund_id
            ).order_by(CapitalCall.call_date).all()
            
            distributions = self.db.query(Distribution).filter(
                Distribution.fund_id == fund_id
            ).order_by(Distribution.distribution_date).all()
            
            adjustments = self.db.query(Adjustment).filter(
                Adjustment.fund_id == fund_id
            ).order_by(Adjustment.adjustment_date).all()
            
            # Return full breakdown with values and transactions
            return {
                "metric": "DPI",
                "formula": "Cumulative Distributions / Paid-In Capital",
                "pic": float(pic) if pic else 0,
                "total_distributions": float(total_distributions) if total_distributions else 0,
                "result": dpi,
                "explanation": f"DPI = {total_distributions} / {pic} = {dpi}",
                "transactions": {
                    "capital_calls": [
                        {
                            "date": str(call.call_date),
                            "amount": float(call.amount),
                            "description": call.description
                        } for call in capital_calls
                    ],
                    "distributions": [
                        {
                            "date": str(dist.distribution_date),
                            "amount": float(dist.amount),
                            "is_recallable": dist.is_recallable,
                            "description": dist.description
                        } for dist in distributions
                    ],
                    "adjustments": [
                        {
                            "date": str(adj.adjustment_date),
                            "amount": float(adj.amount),
                            "type": adj.adjustment_type,
                            "description": adj.description
                        } for adj in adjustments
                    ]
                }
            }
        
        # Handle IRR (Internal Rate of Return)
        elif metric == "irr":
            # Retrieve all inflows and outflows
            cash_flows = self._get_cash_flows(fund_id)
            # Compute IRR
            irr = self.calculate_irr(fund_id)
            
            # Return detailed IRR breakdown
            return {
                "metric": "IRR",
                "formula": "Internal Rate of Return (NPV = 0)",
                "cash_flows": cash_flows,
                "result": irr,
                "explanation": f"IRR calculated from {len(cash_flows)} cash flows = {irr}%",
                "cash_flow_summary": {
                    "total_outflows": sum(cf['amount'] for cf in cash_flows if cf['amount'] < 0),
                    "total_inflows": sum(cf['amount'] for cf in cash_flows if cf['amount'] > 0),
                    "net_cash_flow": sum(cf['amount'] for cf in cash_flows)
                }
            }
        
        # Handle PIC (Paid-In Capital)
        elif metric == "pic":
            # Get detailed capital calls
            capital_calls = self.db.query(CapitalCall).filter(
                CapitalCall.fund_id == fund_id
            ).order_by(CapitalCall.call_date).all()
            
            # Get detailed adjustments
            adjustments = self.db.query(Adjustment).filter(
                Adjustment.fund_id == fund_id
            ).order_by(Adjustment.adjustment_date).all()
            
            total_calls = sum(float(call.amount) for call in capital_calls)
            total_adjustments = sum(float(adj.amount) for adj in adjustments)
            pic = self.calculate_pic(fund_id)
            
            # Return full PIC breakdown
            return {
                "metric": "PIC",
                "formula": "Total Capital Calls - Adjustments",
                "total_calls": total_calls,
                "total_adjustments": total_adjustments,
                "result": float(pic) if pic else 0,
                "explanation": f"PIC = {total_calls} - {total_adjustments} = {pic}",
                "transactions": {
                    "capital_calls": [
                        {
                            "date": str(call.call_date),
                            "amount": float(call.amount),
                            "description": call.description
                        } for call in capital_calls
                    ],
                    "adjustments": [
                        {
                            "date": str(adj.adjustment_date),
                            "amount": float(adj.amount),
                            "type": adj.adjustment_type,
                            "description": adj.description
                        } for adj in adjustments
                    ]
                }
            }
        
        return {"error": "Unknown metric"}
