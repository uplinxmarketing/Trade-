export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  // Allows to automatically instantiate createClient with right options
  // instead of createClient<Database, { PostgrestVersion: 'XX' }>(URL, KEY)
  __InternalSupabase: {
    PostgrestVersion: "14.4"
  }
  public: {
    Tables: {
      ai_analyses: {
        Row: {
          actual_change_percent: number | null
          actual_direction: string | null
          analysis_type: string
          confidence: number
          created_at: string
          id: string
          predicted_change_percent: number | null
          predicted_direction: string | null
          price_at_analysis: number
          reasoning: string | null
          reviewed_at: string | null
          signal: string
          symbol: string
          user_session: string
          was_correct: boolean | null
        }
        Insert: {
          actual_change_percent?: number | null
          actual_direction?: string | null
          analysis_type?: string
          confidence?: number
          created_at?: string
          id?: string
          predicted_change_percent?: number | null
          predicted_direction?: string | null
          price_at_analysis: number
          reasoning?: string | null
          reviewed_at?: string | null
          signal: string
          symbol: string
          user_session?: string
          was_correct?: boolean | null
        }
        Update: {
          actual_change_percent?: number | null
          actual_direction?: string | null
          analysis_type?: string
          confidence?: number
          created_at?: string
          id?: string
          predicted_change_percent?: number | null
          predicted_direction?: string | null
          price_at_analysis?: number
          reasoning?: string | null
          reviewed_at?: string | null
          signal?: string
          symbol?: string
          user_session?: string
          was_correct?: boolean | null
        }
        Relationships: []
      }
      bot_config: {
        Row: {
          created_at: string
          current_balance: number
          id: string
          initial_balance: number
          is_running: boolean
          min_profit_percent: number
          mode: string
          selected_coins: string[]
          stop_loss_percent: number
          updated_at: string
          user_session: string
        }
        Insert: {
          created_at?: string
          current_balance?: number
          id?: string
          initial_balance?: number
          is_running?: boolean
          min_profit_percent?: number
          mode?: string
          selected_coins?: string[]
          stop_loss_percent?: number
          updated_at?: string
          user_session?: string
        }
        Update: {
          created_at?: string
          current_balance?: number
          id?: string
          initial_balance?: number
          is_running?: boolean
          min_profit_percent?: number
          mode?: string
          selected_coins?: string[]
          stop_loss_percent?: number
          updated_at?: string
          user_session?: string
        }
        Relationships: []
      }
      bot_trade_history: {
        Row: {
          bot_id: string | null
          created_at: string
          id: string
          pnl: number | null
          price: number
          quantity: number
          reason: string | null
          side: string
          symbol: string
          user_session: string
        }
        Insert: {
          bot_id?: string | null
          created_at?: string
          id?: string
          pnl?: number | null
          price: number
          quantity: number
          reason?: string | null
          side: string
          symbol: string
          user_session?: string
        }
        Update: {
          bot_id?: string | null
          created_at?: string
          id?: string
          pnl?: number | null
          price?: number
          quantity?: number
          reason?: string | null
          side?: string
          symbol?: string
          user_session?: string
        }
        Relationships: [
          {
            foreignKeyName: "bot_trade_history_bot_id_fkey"
            columns: ["bot_id"]
            isOneToOne: false
            referencedRelation: "trading_bots"
            referencedColumns: ["id"]
          },
        ]
      }
      learning_metrics: {
        Row: {
          accuracy_percent: number
          avg_confidence: number
          best_pattern: string | null
          correct_predictions: number
          id: string
          symbol: string
          total_predictions: number
          updated_at: string
          user_session: string
        }
        Insert: {
          accuracy_percent?: number
          avg_confidence?: number
          best_pattern?: string | null
          correct_predictions?: number
          id?: string
          symbol: string
          total_predictions?: number
          updated_at?: string
          user_session?: string
        }
        Update: {
          accuracy_percent?: number
          avg_confidence?: number
          best_pattern?: string | null
          correct_predictions?: number
          id?: string
          symbol?: string
          total_predictions?: number
          updated_at?: string
          user_session?: string
        }
        Relationships: []
      }
      paper_portfolio: {
        Row: {
          avg_entry_price: number
          bot_id: string | null
          id: string
          quantity: number
          symbol: string
          updated_at: string
          user_session: string
        }
        Insert: {
          avg_entry_price?: number
          bot_id?: string | null
          id?: string
          quantity?: number
          symbol: string
          updated_at?: string
          user_session?: string
        }
        Update: {
          avg_entry_price?: number
          bot_id?: string | null
          id?: string
          quantity?: number
          symbol?: string
          updated_at?: string
          user_session?: string
        }
        Relationships: [
          {
            foreignKeyName: "paper_portfolio_bot_id_fkey"
            columns: ["bot_id"]
            isOneToOne: false
            referencedRelation: "trading_bots"
            referencedColumns: ["id"]
          },
        ]
      }
      paper_trades: {
        Row: {
          created_at: string
          id: string
          price: number
          quantity: number
          side: string
          symbol: string
          total: number | null
          user_session: string
        }
        Insert: {
          created_at?: string
          id?: string
          price: number
          quantity: number
          side: string
          symbol: string
          total?: number | null
          user_session?: string
        }
        Update: {
          created_at?: string
          id?: string
          price?: number
          quantity?: number
          side?: string
          symbol?: string
          total?: number | null
          user_session?: string
        }
        Relationships: []
      }
      trading_bots: {
        Row: {
          allocated_budget: number
          allow_multiple_open: boolean
          assigned_coins: string[]
          available_balance: number
          bot_name: string
          consecutive_losses: number
          cooldown_seconds: number
          created_at: string
          daily_loss: number
          daily_loss_reset_at: string
          fixed_balance_mode: boolean
          id: string
          last_trade_at: string | null
          max_budget_cap: number
          max_daily_loss: number
          max_drawdown_percent: number
          max_trades_per_hour: number
          min_profit_percent: number
          reinvest_profits: boolean
          status: string
          stop_after_consecutive_losses: number
          stop_loss_percent: number
          total_pnl: number
          total_trades: number
          trade_mode: string
          updated_at: string
          used_balance: number
          user_session: string
          winning_trades: number
        }
        Insert: {
          allocated_budget?: number
          allow_multiple_open?: boolean
          assigned_coins?: string[]
          available_balance?: number
          bot_name?: string
          consecutive_losses?: number
          cooldown_seconds?: number
          created_at?: string
          daily_loss?: number
          daily_loss_reset_at?: string
          fixed_balance_mode?: boolean
          id?: string
          last_trade_at?: string | null
          max_budget_cap?: number
          max_daily_loss?: number
          max_drawdown_percent?: number
          max_trades_per_hour?: number
          min_profit_percent?: number
          reinvest_profits?: boolean
          status?: string
          stop_after_consecutive_losses?: number
          stop_loss_percent?: number
          total_pnl?: number
          total_trades?: number
          trade_mode?: string
          updated_at?: string
          used_balance?: number
          user_session?: string
          winning_trades?: number
        }
        Update: {
          allocated_budget?: number
          allow_multiple_open?: boolean
          assigned_coins?: string[]
          available_balance?: number
          bot_name?: string
          consecutive_losses?: number
          cooldown_seconds?: number
          created_at?: string
          daily_loss?: number
          daily_loss_reset_at?: string
          fixed_balance_mode?: boolean
          id?: string
          last_trade_at?: string | null
          max_budget_cap?: number
          max_daily_loss?: number
          max_drawdown_percent?: number
          max_trades_per_hour?: number
          min_profit_percent?: number
          reinvest_profits?: boolean
          status?: string
          stop_after_consecutive_losses?: number
          stop_loss_percent?: number
          total_pnl?: number
          total_trades?: number
          trade_mode?: string
          updated_at?: string
          used_balance?: number
          user_session?: string
          winning_trades?: number
        }
        Relationships: []
      }
    }
    Views: {
      [_ in never]: never
    }
    Functions: {
      [_ in never]: never
    }
    Enums: {
      [_ in never]: never
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">

type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {},
  },
} as const
